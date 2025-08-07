"""
Scraper de vagas do LinkedIn usando JobsPy (Novo) + Selenium (Backup)
JobsPy suporta: LinkedIn, Indeed, Glassdoor, Google, ZipRecruiter
"""

from jobspy import scrape_jobs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import random
from datetime import datetime
from database import DatabaseManager
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LinkedInScraper:
    def __init__(self, usar_jobspy=True):
        self.db = DatabaseManager()
        self.driver = None
        self.usar_jobspy = usar_jobspy
        
        # Configurações JobsPy
        self.termos_busca = [
            "Dados",
            "BI", 
            "Estágio em Dados",
            "Estágio em Engenheiro de Dados",
            "Estágio em Ciência de Dados",
            "Estágio em BI"
        ]
        
        self.sites_jobspy = ["linkedin", "indeed", "google"]  # glassdoor, ziprecruiter, google pode dar problema
        
        # URLs de busca antigas (backup)
        self.urls_busca = [
            {
                'url': 'https://www.linkedin.com/jobs/search/?currentJobId=4280941701&distance=25&f_E=1&f_TPR=r14000&f_WT=2%2C3&geoId=104746682&keywords=Dados&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true',
                'keyword': 'Dados'
            }
        ]
    
    def fazer_scraping_jobspy(self):
        """Método principal usando JobsPy"""
        logger.info("🚀 INICIANDO SCRAPING COM JOBSPY")
        logger.info("Sites: LinkedIn, Indeed, ZipRecruiter, Google")
        logger.info("=" * 50)
        
        total_novas_vagas = 0
        
        for termo in self.termos_busca:
            logger.info(f"\n🔍 Buscando vagas para: '{termo}'")
            
            try:
                # Configurar busca
                google_search_term = f"{termo} empregos São Paulo últimos dias"
                
                # Fazer scraping
                jobs_df = scrape_jobs(
                    site_name=self.sites_jobspy,
                    search_term=termo,
                    google_search_term=google_search_term,
                    location="São Paulo, SP",
                    results_wanted=20,
                    hours_old=2,  # últimas 48 horas
                    country_indeed="Brazil",
                    linkedin_fetch_description=True,
                    verbose=1
                )
                
                if jobs_df is None or jobs_df.empty:
                    logger.warning(f"❌ Nenhuma vaga encontrada para '{termo}'")
                    continue
                
                logger.info(f"✅ {len(jobs_df)} vagas encontradas para '{termo}'")
                
                # Processar e salvar vagas
                novas_vagas = 0
                for _, job_row in jobs_df.iterrows():
                    vaga_data = self.processar_vaga_jobspy(job_row, termo)
                    
                    if vaga_data and self.db.inserir_vaga(vaga_data):
                        novas_vagas += 1
                        logger.info(f"  💾 {vaga_data['titulo']} - {vaga_data['empresa']} ({vaga_data['site_origem']})")
                
                total_novas_vagas += novas_vagas
                logger.info(f"✅ '{termo}': {novas_vagas} novas vagas salvas")
                
                # Pausa entre termos
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                logger.error(f"❌ Erro para termo '{termo}': {e}")
                continue
        
        logger.info(f"\n🎉 Scraping JobsPy concluído: {total_novas_vagas} novas vagas")
        return total_novas_vagas
    
    def processar_vaga_jobspy(self, job_row, termo_busca):
        """Processa vaga do JobsPy para formato do banco"""
        try:
            # Converter para dict
            if hasattr(job_row, 'to_dict'):
                job_data = job_row.to_dict()
            else:
                job_data = job_row
            
            # Extrair campos principais
            titulo = str(job_data.get('title', '')).strip()
            empresa = str(job_data.get('company', '')).strip()
            link = str(job_data.get('job_url', '')).strip()
            
            # Validação básica
            if not titulo or not empresa or not link or not link.startswith('http'):
                return None
            
            # Outros campos
            localizacao = str(job_data.get('location', 'São Paulo, SP')).strip()
            descricao = str(job_data.get('description', 'Descrição não disponível')).strip()[:1200]
            site_origem = str(job_data.get('site', 'Desconhecido'))
            data_postagem = str(job_data.get('date_posted', 'Não informado'))
            
            # Campos específicos JobsPy
            job_type = str(job_data.get('job_type', 'Não informado'))
            is_remote = str(job_data.get('is_remote', 'Não informado'))
            
            # Salário
            salary_info = "Não informado"
            min_amount = job_data.get('min_amount')
            max_amount = job_data.get('max_amount')
            interval = job_data.get('interval', '')
            
            if min_amount or max_amount:
                parts = []
                if min_amount:
                    parts.append(f"R$ {min_amount}")
                if max_amount:
                    parts.append(f"R$ {max_amount}")
                if interval:
                    parts.append(f"/{interval}")
                salary_info = " - ".join(parts)
            
            return {
                'titulo': titulo,
                'empresa': empresa,
                'localizacao': localizacao,
                'area_vaga': localizacao,
                'descricao': descricao,
                'link': link,
                'data_postagem': data_postagem,
                'numero_candidatos': 'Não informado',
                'site_origem': site_origem,
                'job_type': job_type,
                'is_remote': is_remote,
                'salary_info': salary_info,
                'keyword_busca': termo_busca
            }
            
        except Exception as e:
            logger.error(f"Erro ao processar vaga: {e}")
            return None
    
    def configurar_driver(self):
        """Configura o driver do Chrome com opções otimizadas"""
        options = Options()
        options.add_argument('--headless')  # Executar em modo headless
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Configurações experimentais para evitar detecção
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        try:
            # Usar Selenium Manager (funciona melhor que WebDriverManager)
            logger.info("Configurando ChromeDriver via Selenium Manager...")
            self.driver = webdriver.Chrome(options=options)
            logger.info("✅ ChromeDriver configurado com sucesso!")
            
            # Configurações adicionais para evitar detecção
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
        except Exception as e:
            logger.error(f"❌ Erro ao configurar ChromeDriver: {e}")
            logger.error("Verifique se o Google Chrome está instalado corretamente")
            raise Exception("Falha na configuração do ChromeDriver")
        
        return self.driver
    
    def extrair_vagas_pagina(self, keyword):
        """Extrai vagas da página atual"""
        vagas_extraidas = []
        
        try:
            # Aguardar carregamento das vagas
            wait = WebDriverWait(self.driver, 10)
            vagas_elementos = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-job-id]"))
            )
            
            logger.info(f"Encontradas {len(vagas_elementos)} vagas na página para keyword '{keyword}'")
            
            for vaga_elemento in vagas_elementos[:10]:  # Limitar a 10 vagas por página
                try:
                    # Obter ID da vaga
                    job_id = vaga_elemento.get_attribute('data-job-id')
                    
                    # Clicar na vaga para carregar detalhes
                    self.driver.execute_script("arguments[0].click();", vaga_elemento)
                    time.sleep(random.uniform(1, 3))
                    
                    # Extrair informações básicas
                    titulo_elemento = vaga_elemento.find_element(By.CSS_SELECTOR, "h3 a")
                    titulo = titulo_elemento.text.strip()
                    link = titulo_elemento.get_attribute('href')
                    
                    # Empresa
                    try:
                        empresa_elemento = vaga_elemento.find_element(By.CSS_SELECTOR, "h4 a")
                        empresa = empresa_elemento.text.strip()
                    except NoSuchElementException:
                        empresa = "Não informado"
                    
                    # Localização
                    try:
                        localizacao_elemento = vaga_elemento.find_element(By.CSS_SELECTOR, "[data-test-id='job-search-card-location']")
                        localizacao = localizacao_elemento.text.strip()
                    except NoSuchElementException:
                        localizacao = "Não informado"
                    
                    # Data de postagem
                    try:
                        data_elemento = vaga_elemento.find_element(By.CSS_SELECTOR, "time")
                        data_postagem = data_elemento.get_attribute('datetime')
                        if not data_postagem:
                            data_postagem = data_elemento.text.strip()
                    except NoSuchElementException:
                        data_postagem = "Não informado"
                    
                    # Descrição (do painel lateral)
                    descricao = self.extrair_descricao_detalhada()
                    
                    vaga_data = {
                        'titulo': titulo,
                        'empresa': empresa,
                        'localizacao': localizacao,
                        'descricao': descricao,
                        'link': link,
                        'data_postagem': data_postagem,
                        'keyword_busca': keyword
                    }
                    
                    vagas_extraidas.append(vaga_data)
                    logger.info(f"Vaga extraída: {titulo} - {empresa}")
                    
                except Exception as e:
                    logger.error(f"Erro ao extrair vaga individual: {e}")
                    continue
        
        except TimeoutException:
            logger.warning("Timeout ao aguardar carregamento das vagas")
        except Exception as e:
            logger.error(f"Erro ao extrair vagas da página: {e}")
        
        return vagas_extraidas
    
    def extrair_descricao_detalhada(self):
        """Extrai a descrição detalhada da vaga do painel lateral"""
        try:
            # Aguardar carregamento da descrição
            wait = WebDriverWait(self.driver, 5)
            descricao_elemento = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".jobs-search__job-details--container"))
            )
            
            # Tentar diferentes seletores para a descrição
            seletores_descricao = [
                ".jobs-box__html-content",
                ".jobs-description-content__text",
                ".jobs-description__content",
                "[data-test-id='job-details-description']"
            ]
            
            for seletor in seletores_descricao:
                try:
                    desc_element = descricao_elemento.find_element(By.CSS_SELECTOR, seletor)
                    return desc_element.text.strip()[:1000]  # Limitar a 1000 caracteres
                except NoSuchElementException:
                    continue
            
            # Se não encontrar com seletores específicos, pegar texto geral
            return descricao_elemento.text.strip()[:500]
            
        except (TimeoutException, NoSuchElementException):
            return "Descrição não disponível"
    
    def fazer_scraping(self):
        """Método principal - usa JobsPy por padrão, Selenium como backup"""
        if self.usar_jobspy:
            try:
                logger.info("🌟 Usando JobsPy (Recomendado)")
                return self.fazer_scraping_jobspy()
            except Exception as e:
                logger.error(f"❌ JobsPy falhou: {e}")
                logger.info("🔄 Fallback para Selenium...")
                self.usar_jobspy = False
                return self.fazer_scraping_selenium()
        else:
            logger.info("🔧 Usando Selenium (Backup)")
            return self.fazer_scraping_selenium()
    
    def fazer_scraping_selenium(self):
        """Método original usando Selenium (backup)"""
        total_novas_vagas = 0
        
        try:
            self.configurar_driver()
            
            for busca in self.urls_busca:
                logger.info(f"Iniciando scraping para keyword: {busca['keyword']}")
                
                try:
                    self.driver.get(busca['url'])
                    time.sleep(random.uniform(3, 6))
                    
                    # Aceitar cookies se aparecer
                    try:
                        accept_cookies = WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Aceitar')]"))
                        )
                        accept_cookies.click()
                        time.sleep(2)
                    except TimeoutException:
                        pass
                    
                    # Extrair vagas
                    vagas = self.extrair_vagas_pagina(busca['keyword'])
                    
                    # Salvar no banco de dados
                    novas_vagas = 0
                    for vaga in vagas:
                        if self.db.inserir_vaga(vaga):
                            novas_vagas += 1
                    
                    total_novas_vagas += novas_vagas
                    logger.info(f"Keyword '{busca['keyword']}': {novas_vagas} novas vagas adicionadas")
                    
                    # Pausa entre URLs
                    time.sleep(random.uniform(5, 10))
                    
                except Exception as e:
                    logger.error(f"Erro ao processar URL para keyword '{busca['keyword']}': {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Erro geral no scraping: {e}")
        
        finally:
            if self.driver:
                self.driver.quit()
        
        logger.info(f"Scraping Selenium concluído. Total de novas vagas: {total_novas_vagas}")
        return total_novas_vagas

def executar_scraping(usar_jobspy=True):
    """Função para executar o scraping - pode ser chamada pelo scheduler"""
    scraper = LinkedInScraper(usar_jobspy=usar_jobspy)
    return scraper.fazer_scraping()

def executar_scraping_jobspy():
    """Função específica para JobsPy"""
    return executar_scraping(usar_jobspy=True)

def executar_scraping_selenium():
    """Função específica para Selenium"""
    return executar_scraping(usar_jobspy=False)

if __name__ == "__main__":
    # Por padrão, usar JobsPy
    logger.info("🚀 Iniciando scraper com JobsPy...")
    resultado = executar_scraping_jobspy()
    logger.info(f"✅ Scraping finalizado: {resultado} novas vagas")
