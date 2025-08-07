"""
Scraper moderno usando JobsPy para múltiplas plataformas:
LinkedIn, Indeed, Glassdoor, Google e ZipRecruiter
"""

import csv
import pandas as pd
from jobspy import scrape_jobs
from datetime import datetime, timedelta
import logging
import time
import random
from database import DatabaseManager
import hashlib

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JobSpyScraper:
    def __init__(self):
        self.db = DatabaseManager()
        
        # Configurações de busca
        self.termos_busca = [
            "Dados",
            "BI", 
            "Estágio em Dados",
            "Estágio em Engenheiro de Dados",
            "Estágio em Ciência de Dados",
            "Estágio em BI"
        ]
        
        # Sites para buscar (com tratamento específico para cada um)
        self.sites = ["linkedin", "indeed", "google", "glassdoor", "ziprecruiter"]
        
        # Configurações
        self.location = "São Paulo, SP, Brasil"
        self.results_wanted = 10  # Reduzir para evitar rate limiting
        self.hours_old = 24  # Vagas das últimas 24 horas
        self.country_indeed = "Brazil"
        
        # Configurações específicas para sites problemáticos
        self.max_retries = 2  # Número máximo de tentativas por site
        self.glassdoor_delay = 3  # Delay extra para Glassdoor (segundos)
        self.ziprecruiter_enabled = True  # Controle para habilitar/desabilitar ZipRecruiter
        
    def gerar_google_search_term(self, search_term):
        """Gera termo de busca específico para Google Jobs"""
        return f"{search_term} jobs near São Paulo, Brazil since yesterday"
    
    def limpar_e_validar_dados(self, job_data):
        """Limpa e valida os dados extraídos"""
        try:
            # Converter para dicionário se for Series do pandas
            if hasattr(job_data, 'to_dict'):
                job_data = job_data.to_dict()
            
            # Limpar dados
            titulo = str(job_data.get('title', 'Não informado')).strip()
            empresa = str(job_data.get('company', 'Não informado')).strip()
            localizacao = str(job_data.get('location', 'Não informado')).strip()
            descricao = str(job_data.get('description', 'Não informado')).strip()
            link = str(job_data.get('job_url', 'Não informado')).strip()
            site_origem = str(job_data.get('site', 'Não informado')).strip()
            
            # Data de postagem
            data_postagem = job_data.get('date_posted')
            if data_postagem:
                data_postagem = str(data_postagem)
            else:
                data_postagem = "Não informado"
            
            # Informações adicionais específicas
            job_type = str(job_data.get('job_type', 'Não informado')).strip()
            is_remote = str(job_data.get('is_remote', 'Não informado')).strip()
            
            # Salário
            salary_info = "Não informado"
            if job_data.get('min_amount') or job_data.get('max_amount'):
                min_sal = job_data.get('min_amount', '')
                max_sal = job_data.get('max_amount', '')
                interval = job_data.get('interval', '')
                salary_info = f"{min_sal}-{max_sal} {interval}".strip()
            
            # Validar campos obrigatórios
            if not titulo or titulo == 'nan':
                return None
            if not empresa or empresa == 'nan':
                return None
            if not link or link == 'nan' or not link.startswith('http'):
                return None
            
            return {
                'titulo': titulo,
                'empresa': empresa,
                'localizacao': localizacao,
                'area_vaga': localizacao,  # Usar localização como área por enquanto
                'descricao': descricao[:1500],  # Limitar descrição
                'link': link,
                'data_postagem': data_postagem,
                'numero_candidatos': "Não informado",  # JobsPy não retorna esse campo
                'site_origem': site_origem,
                'job_type': job_type,
                'is_remote': is_remote,
                'salary_info': salary_info,
                'keyword_busca': '',  # Será preenchido na função de scraping
                'estado': '',  # Será preenchido na função de scraping
                'local_busca': ''  # Será preenchido na função de scraping
            }
            
        except Exception as e:
            logger.error(f"Erro ao limpar dados da vaga: {e}")
            return None
    
    def fazer_scraping_termo(self, search_term):
        """Faz scraping para um termo específico"""
        logger.info(f"🔍 Iniciando scraping para termo: '{search_term}'")
        
        vagas_todas = []
        sites_a_tentar = self.sites.copy()
        
        # Verificar se ZipRecruiter está habilitado
        if not self.ziprecruiter_enabled:
            if "ziprecruiter" in sites_a_tentar:
                sites_a_tentar.remove("ziprecruiter")
                logger.info("⚠️ ZipRecruiter desabilitado temporariamente (rate limiting)")
        
        # Tentar cada site separadamente para melhor controle de erros
        for site in sites_a_tentar:
            retry_count = 0
            site_success = False
            
            while retry_count < self.max_retries and not site_success:
                try:
                    logger.info(f"🌐 Tentando site: {site} (tentativa {retry_count + 1}/{self.max_retries})")
                    
                    # Configurar termo para Google
                    termo_busca = search_term
                    if site == 'google':
                        termo_busca = self.gerar_google_search_term(search_term)
                    
                    # Ajustes específicos por site
                    site_config = {}
                    
                    if site == 'linkedin':
                        site_config['linkedin_fetch_description'] = True
                    
                    if site == 'indeed':
                        site_config['country_indeed'] = self.country_indeed
                    
                    if site == 'glassdoor':
                        # Reduzir expectations para Glassdoor (menos resultados, menos dados)
                        site_results = max(3, self.results_wanted // 2)
                        # Adicionar pausa extra antes do glassdoor
                        time.sleep(self.glassdoor_delay)
                    else:
                        site_results = self.results_wanted
                    
                    if site == 'ziprecruiter':
                        # Reduzir expectations para ZipRecruiter (poucos resultados)
                        site_results = max(2, self.results_wanted // 3)
                    
                    # Fazer scraping individual com timeout
                    try:
                        jobs_df = scrape_jobs(
                            site_name=[site],  # Um site por vez
                            search_term=termo_busca,
                            location=self.location,
                            results_wanted=site_results,
                            hours_old=self.hours_old,
                            verbose=0,  # Reduzir logs
                            **site_config
                        )
                        
                        if jobs_df is not None and not jobs_df.empty:
                            logger.info(f"✅ {len(jobs_df)} vagas encontradas em {site}")
                            
                            # Processar vagas deste site
                            for index, job_row in jobs_df.iterrows():
                                vaga_limpa = self.limpar_e_validar_dados(job_row)
                                
                                if vaga_limpa:
                                    vaga_limpa['keyword_busca'] = search_term
                                    vagas_todas.append(vaga_limpa)
                                    logger.info(f"  📝 {vaga_limpa['titulo']} - {vaga_limpa['empresa']} ({site})")
                            
                            site_success = True
                        else:
                            logger.warning(f"⚠️ Nenhuma vaga encontrada em {site}")
                            site_success = True  # Considerar sucesso mesmo sem resultados
                    
                    except Exception as site_error:
                        # Tratar erros específicos por site
                        if site == 'ziprecruiter' and '429' in str(site_error):
                            logger.error(f"⛔ ZipRecruiter está bloqueando por rate limiting. Desabilitando temporariamente.")
                            self.ziprecruiter_enabled = False
                            break  # Sair do loop de retry para este site
                        
                        if site == 'glassdoor' and ('400' in str(site_error) or 'location not parsed' in str(site_error).lower()):
                            logger.error(f"⛔ Glassdoor erro 400 ou problema de localização. Ajustando parâmetros.")
                            # Tentar ajustar a localização para próxima tentativa
                            self.location = "São Paulo, Brasil" if retry_count == 0 else "São Paulo"
                        
                        logger.error(f"❌ Erro no site {site} (tentativa {retry_count + 1}): {site_error}")
                        retry_count += 1
                        
                        # Esperar mais tempo entre retentativas
                        backoff_time = 5 + (retry_count * 3)
                        logger.info(f"⏱️ Aguardando {backoff_time}s antes de tentar novamente...")
                        time.sleep(backoff_time)
                
                except Exception as e:
                    logger.error(f"❌ Erro geral no site {site}: {e}")
                    retry_count += 1
            
            # Pausa entre sites
            next_site_delay = random.uniform(3, 8)
            logger.info(f"⏸️ Pausando {next_site_delay:.1f}s antes do próximo site...")
            time.sleep(next_site_delay)
        
        # Restaurar configuração de local padrão caso tenha sido alterada
        self.location = "São Paulo, SP, Brasil"
        
        logger.info(f"✅ Total processado para '{search_term}': {len(vagas_todas)} vagas")
        return vagas_todas
    
    def fazer_scraping(self):
        """Executa o processo de scraping para todos os termos e todas as localizações"""
        logger.info("🚀 Iniciando processo de scraping...")
        start_time = time.time()
        
        # Lista de resultados para todos os termos e localizações
        resultados_finais = []
        
        # Tentar com diferentes formatos de localização se múltiplas estiverem configuradas
        locations = []
        if isinstance(self.location, str):
            # Localização única
            locations = [self.location]
        elif isinstance(self.location, list):
            # Lista de localizações
            locations = self.location
        else:
            # Fallback para SP
            locations = ["São Paulo, SP, Brasil"]
            logger.warning("⚠️ Formato de localização inválido, usando São Paulo como padrão")

        # Usar no máximo 3 localizações para evitar bloqueios
        if len(locations) > 3:
            logger.warning(f"⚠️ Muitas localizações ({len(locations)}), limitando a 3 para evitar bloqueios")
            locations = locations[:3]
        
        logger.info(f"🌎 Buscando em {len(locations)} localização(ões): {locations}")
        
        # Para cada localização
        for loc_index, current_location in enumerate(locations):
            logger.info(f"🌍 Processando localização {loc_index+1}/{len(locations)}: {current_location}")
            
            # Atualizar localização atual
            self.location = current_location
            
            # Para cada termo de busca
            for term_index, search_term in enumerate(self.termos_busca):
                logger.info(f"🔍 Termo {term_index+1}/{len(self.termos_busca)}: '{search_term}' em '{current_location}'")
                
                try:
                    vagas_termo = self.fazer_scraping_termo(search_term)
                    
                    # Adicionar informação de localização explícita
                    for vaga in vagas_termo:
                        vaga['local_busca'] = current_location
                        
                        # Extrair estado da localização (se possível)
                        estado = None
                        if "," in current_location:
                            partes = current_location.split(",")
                            if len(partes) >= 2:
                                estado_part = partes[1].strip()
                                # Verificar se é um código de estado (SP, RJ, etc)
                                if len(estado_part) <= 3:
                                    estado = estado_part
                        
                        vaga['estado'] = estado
                        resultados_finais.append(vaga)
                    
                    logger.info(f"✅ Encontradas {len(vagas_termo)} vagas para '{search_term}' em '{current_location}'")
                
                except Exception as e:
                    logger.error(f"❌ Erro ao processar termo '{search_term}' em '{current_location}': {e}")
                
                # Pausa entre termos de busca
                if term_index < len(self.termos_busca) - 1:
                    pausa = random.uniform(5, 8)
                    logger.info(f"⏸️ Pausando {pausa:.1f}s antes do próximo termo...")
                    time.sleep(pausa)
            
            # Pausa maior entre localizações
            if loc_index < len(locations) - 1:
                pausa = random.uniform(8, 15)
                logger.info(f"⏸️ Pausando {pausa:.1f}s antes da próxima localização...")
                time.sleep(pausa)
        
        # Converte para DataFrame
        if resultados_finais:
            df_final = pd.DataFrame(resultados_finais)
            
            # Calcular hash_id para cada vaga
            if 'hash_id' not in df_final.columns:
                logger.info("🔑 Gerando hash_id para cada vaga...")
                
                # Função para gerar hash único para cada vaga
                def gerar_hash_id(row):
                    dados = f"{row['titulo']}{row['empresa']}{row['link']}"
                    return hashlib.md5(dados.encode('utf-8')).hexdigest()
                
                df_final['hash_id'] = df_final.apply(gerar_hash_id, axis=1)
            
            # Remove duplicatas com base no hash_id
            df_antes = len(df_final)
            df_final = df_final.drop_duplicates(subset=['hash_id'])
            
            duplicatas = df_antes - len(df_final)
            logger.info(f"✅ Removidas {duplicatas} duplicatas")
            logger.info(f"✅ Total após remoção de duplicatas: {len(df_final)} vagas")
            
            # Calcular tempo total
            tempo_total = time.time() - start_time
            logger.info(f"⏱️ Tempo total de scraping: {tempo_total:.1f} segundos")
            
            return df_final
        else:
            logger.warning("⚠️ Nenhuma vaga encontrada em nenhum site!")
            return pd.DataFrame()
    
    def fazer_scraping_completo(self):
        """Executa scraping completo para todos os termos"""
        logger.info("🚀 INICIANDO SCRAPING COMPLETO COM JOBSPY")
        logger.info("=" * 60)
        
        total_novas_vagas = 0
        
        # Obter DataFrame com todos os resultados
        df_vagas = self.fazer_scraping()
        
        # Salvar vagas no banco
        if not df_vagas.empty:
            for _, vaga in df_vagas.iterrows():
                try:
                    vaga_dict = vaga.to_dict()
                    if self.db.inserir_vaga(vaga_dict):
                        total_novas_vagas += 1
                except Exception as e:
                    logger.error(f"Erro ao inserir vaga no banco: {e}")
            
            # Relatório final
            logger.info("=" * 60)
            logger.info(f"🎉 SCRAPING CONCLUÍDO!")
            logger.info(f"📊 Total de vagas encontradas: {len(df_vagas)}")
            logger.info(f"💾 Total de novas vagas salvas: {total_novas_vagas}")
            
            # Estatísticas por site
            sites_stats = df_vagas['site_origem'].value_counts().to_dict()
            
            logger.info("\n📈 ESTATÍSTICAS POR SITE:")
            for site, count in sorted(sites_stats.items()):
                logger.info(f"  {site}: {count} vagas")
            
            # Estatísticas por localização
            if 'local_busca' in df_vagas.columns:
                loc_stats = df_vagas['local_busca'].value_counts().to_dict()
                
                logger.info("\n� ESTATÍSTICAS POR LOCALIZAÇÃO:")
                for loc, count in sorted(loc_stats.items()):
                    logger.info(f"  {loc}: {count} vagas")
        
        return total_novas_vagas
    
    def salvar_csv_backup(self, todas_vagas):
        """Salva backup em CSV"""
        try:
            if todas_vagas:
                df = pd.DataFrame(todas_vagas)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"vagas_backup_{timestamp}.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                logger.info(f"💾 Backup salvo em: {filename}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar backup: {e}")

def executar_scraping_jobspy():
    """Função principal para executar o scraping"""
    scraper = JobSpyScraper()
    return scraper.fazer_scraping_completo()

def teste_jobspy():
    """Teste rápido do JobsPy"""
    logger.info("🧪 TESTE RÁPIDO DO JOBSPY")
    
    try:
        # Teste simples com um termo
        jobs = scrape_jobs(
            site_name=["indeed"],  # Apenas Indeed para teste
            search_term="Dados",
            location="São Paulo",
            results_wanted=5,
            hours_old=168,  # 1 semana
            country_indeed="Brazil",
            verbose=1
        )
        
        if jobs is not None and not jobs.empty:
            logger.info(f"✅ Teste OK: {len(jobs)} vagas encontradas")
            logger.info("📋 Colunas disponíveis:")
            for col in jobs.columns:
                logger.info(f"  - {col}")
            return True
        else:
            logger.warning("❌ Teste falhou: Nenhuma vaga encontrada")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro no teste: {e}")
        return False

if __name__ == "__main__":
    # Primeiro fazer um teste
    if teste_jobspy():
        # Se o teste passou, executar scraping completo
        executar_scraping_jobspy()
    else:
        logger.error("❌ Teste falhou. Verifique a configuração.")
