"""
App Streamlit com DataTable interativa e funcionalidades de exclusão
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from scraper import executar_scraping_jobspy, executar_scraping_selenium
import threading
import asyncio

# Configuração da página
st.set_page_config(
    page_title="SearchVagas - Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    
    .delete-button {
        background-color: #ff4b4b;
        color: white;
        border: none;
        padding: 5px 10px;
        border-radius: 5px;
        cursor: pointer;
    }
    
    .delete-button:hover {
        background-color: #ff6b6b;
    }
    
    .dataframe {
        font-size: 12px;
    }
    
    .success-msg {
        padding: 10px;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        color: #155724;
    }
    
    .warning-msg {
        padding: 10px;
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        color: #856404;
    }
    
    .vaga-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
        margin: 10px 0;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        transition: transform 0.3s ease;
    }
    
    .vaga-card:hover {
        transform: translateY(-5px);
    }
    
    .vaga-title {
        font-size: 1.2em;
        font-weight: bold;
        margin-bottom: 10px;
    }
    
    .vaga-info {
        background-color: rgba(255,255,255,0.1);
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .link-button {
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
        color: white;
        padding: 8px 16px;
        border-radius: 20px;
        text-decoration: none;
        display: inline-block;
        margin: 5px;
        transition: all 0.3s ease;
    }
    
    .link-button:hover {
        transform: scale(1.05);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    
    .descricao-box {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin: 15px 0;
        max-height: 300px;
        overflow-y: auto;
        color: #000000;
        line-height: 1.6;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
    }
    
    .descricao-box p {
        color: #000000 !important;
        margin-bottom: 10px;
    }
    
    .descricao-box ul, .descricao-box ol {
        color: #000000 !important;
        padding-left: 20px;
    }
    
    .descricao-box li {
        color: #000000 !important;
        margin-bottom: 5px;
    }
</style>
""", unsafe_allow_html=True)

class StreamlitAppAvancado:
    def __init__(self):
        self.db_path = "vagas_linkedin.db"
        
    def conectar_db(self):
        """Conecta ao banco de dados"""
        return sqlite3.connect(self.db_path)
    
    def deletar_vaga(self, vaga_id):
        """Deleta uma vaga específica"""
        try:
            conn = self.conectar_db()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vagas WHERE id = ?", (vaga_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao deletar vaga: {e}")
            return False
    
    def deletar_todas_vagas(self):
        """Deleta todas as vagas do banco"""
        try:
            conn = self.conectar_db()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vagas")
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao deletar todas as vagas: {e}")
            return False
    
    def extrair_estados_cidades(self):
        """Extrai estados e cidades únicos das localizações"""
        conn = self.conectar_db()
        
        try:
            # Buscar todas as localizações únicas
            df_localizacoes = pd.read_sql_query("""
                SELECT DISTINCT localizacao 
                FROM vagas 
                WHERE localizacao IS NOT NULL 
                AND localizacao != 'Não informado'
                AND localizacao != ''
            """, conn)
            
            estados = set()
            cidades = set()
            
            for localizacao in df_localizacoes['localizacao']:
                if pd.isna(localizacao) or not localizacao:
                    continue
                
                # Padrões comuns: "São Paulo, SP - Brasil", "Rio de Janeiro, RJ - Brasil", "São Paulo, SP, BR"
                localizacao_limpa = str(localizacao).strip()
                
                # Remover país do final
                for sufixo in [' - Brasil', ' - Brazil', ', BR', ', Brazil']:
                    if localizacao_limpa.endswith(sufixo):
                        localizacao_limpa = localizacao_limpa[:-len(sufixo)].strip()
                
                # Dividir por vírgula
                partes = [parte.strip() for parte in localizacao_limpa.split(',')]
                
                if len(partes) >= 2:
                    # Primeira parte é geralmente a cidade
                    cidade = partes[0].strip()
                    if cidade and len(cidade) > 1:
                        cidades.add(cidade)
                    
                    # Segunda parte é geralmente o estado
                    estado = partes[1].strip()
                    if estado and len(estado) > 0:
                        # Se for sigla do estado (2 caracteres), manter como está
                        # Se for nome completo, também manter
                        estados.add(estado)
            
            conn.close()
            
            return {
                'estados': sorted(list(estados)),
                'cidades': sorted(list(cidades))
            }
            
        except Exception as e:
            st.error(f"Erro ao extrair estados e cidades: {e}")
            conn.close()
            return {'estados': [], 'cidades': []}
    
    def obter_vagas_dataframe(self, limit=None, horas_recentes=None, filtros=None):
        """Obtém vagas como DataFrame para a datatable"""
        conn = self.conectar_db()
        
        query = """
        SELECT id, titulo, empresa, localizacao, descricao, link, data_postagem, 
               data_coleta, keyword_busca, area_vaga, numero_candidatos, 
               site_origem, job_type, is_remote, salary_info
        FROM vagas
        """
        
        conditions = []
        params = []
        
        if horas_recentes:
            conditions.append("data_coleta >= datetime('now', '-{} hours')".format(horas_recentes))
        
        # Aplicar filtros adicionais
        if filtros:
            if filtros.get('empresa') and filtros['empresa'] != 'Todas':
                conditions.append("empresa = ?")
                params.append(filtros['empresa'])
            
            if filtros.get('site') and filtros['site'] != 'Todos':
                conditions.append("site_origem = ?")
                params.append(filtros['site'])
            
            if filtros.get('keyword') and filtros['keyword'] != 'Todas':
                conditions.append("keyword_busca = ?")
                params.append(filtros['keyword'])
            
            if filtros.get('job_type') and filtros['job_type'] != 'Todos':
                conditions.append("job_type = ?")
                params.append(filtros['job_type'])
            
            if filtros.get('is_remote') and filtros['is_remote'] != 'Todos':
                conditions.append("is_remote = ?")
                params.append(filtros['is_remote'])
            
            # Filtro por horário flexível
            if filtros.get('horario_flexivel') and filtros['horario_flexivel'] != 'Todos':
                if filtros['horario_flexivel'] == 'True':
                    conditions.append("""(
                        LOWER(descricao) LIKE '%horário flexível%' OR 
                        LOWER(descricao) LIKE '%horario flexivel%' OR 
                        LOWER(descricao) LIKE '%flexible schedule%' OR 
                        LOWER(descricao) LIKE '%flexibilidade de horário%' OR 
                        LOWER(descricao) LIKE '%flexibilidade horário%' OR 
                        LOWER(descricao) LIKE '%horários flexíveis%' OR 
                        LOWER(descricao) LIKE '%horarios flexiveis%'
                    )""")
                else:  # False
                    conditions.append("""NOT (
                        LOWER(descricao) LIKE '%horário flexível%' OR 
                        LOWER(descricao) LIKE '%horario flexivel%' OR 
                        LOWER(descricao) LIKE '%flexible schedule%' OR 
                        LOWER(descricao) LIKE '%flexibilidade de horário%' OR 
                        LOWER(descricao) LIKE '%flexibilidade horário%' OR 
                        LOWER(descricao) LIKE '%horários flexíveis%' OR 
                        LOWER(descricao) LIKE '%horarios flexiveis%'
                    )""")
            
            # Filtro por estados (múltipla seleção)
            if filtros.get('estados') and filtros['estados']:
                estados_conditions = []
                for estado in filtros['estados']:
                    estados_conditions.append(f"localizacao LIKE '%{estado}%'")
                if estados_conditions:
                    conditions.append(f"({' OR '.join(estados_conditions)})")
            
            # Filtro por cidades (múltipla seleção)
            if filtros.get('cidades') and filtros['cidades']:
                cidades_conditions = []
                for cidade in filtros['cidades']:
                    cidades_conditions.append(f"localizacao LIKE '%{cidade}%'")
                if cidades_conditions:
                    conditions.append(f"({' OR '.join(cidades_conditions)})")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY data_coleta DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            # Tratar valores None/null em todo o DataFrame
            if not df.empty:
                # Substituir valores None por strings vazias ou valores padrão
                df['titulo'] = df['titulo'].fillna('Sem título')
                df['empresa'] = df['empresa'].fillna('Não informado')
                df['site_origem'] = df['site_origem'].fillna('Não informado')
                df['localizacao'] = df['localizacao'].fillna('Não informado')
                df['job_type'] = df['job_type'].fillna('Não informado')
                df['is_remote'] = df['is_remote'].fillna('Não informado')
                df['salary_info'] = df['salary_info'].fillna('Não informado')
                df['keyword_busca'] = df['keyword_busca'].fillna('Não informado')
                df['data_postagem'] = df['data_postagem'].fillna('Não informado')
                df['numero_candidatos'] = df['numero_candidatos'].fillna('0')
                df['descricao'] = df['descricao'].fillna('Sem descrição')
                df['link'] = df['link'].fillna('')
                df['area_vaga'] = df['area_vaga'].fillna('Não informado')
                
                # Adicionar coluna de ação (será usada para botões)
                df['Ações'] = df['id'].apply(lambda x: f"delete_{x}")
            
            return df
        except Exception as e:
            st.error(f"Erro ao obter dados: {e}")
            conn.close()
            return pd.DataFrame()
    
    def obter_estatisticas(self):
        """Obtém estatísticas das vagas"""
        conn = self.conectar_db()
        
        stats = {}
        
        try:
            # Total de vagas
            stats['total'] = pd.read_sql_query("SELECT COUNT(*) as count FROM vagas", conn).iloc[0]['count']
            
            # Vagas por site
            stats['por_site'] = pd.read_sql_query("""
                SELECT COALESCE(site_origem, 'Não informado') as site_origem, COUNT(*) as count 
                FROM vagas 
                GROUP BY COALESCE(site_origem, 'Não informado')
                ORDER BY count DESC
            """, conn)
            
            # Vagas por empresa (top 10)
            stats['por_empresa'] = pd.read_sql_query("""
                SELECT COALESCE(empresa, 'Não informado') as empresa, COUNT(*) as count 
                FROM vagas 
                GROUP BY COALESCE(empresa, 'Não informado')
                ORDER BY count DESC 
                LIMIT 10
            """, conn)
            
            # Vagas por tipo de trabalho
            stats['por_tipo'] = pd.read_sql_query("""
                SELECT COALESCE(job_type, 'Não informado') as job_type, COUNT(*) as count 
                FROM vagas 
                WHERE COALESCE(job_type, 'Não informado') != 'Não informado'
                GROUP BY COALESCE(job_type, 'Não informado')
                ORDER BY count DESC
            """, conn)
            
            # Vagas remotas
            stats['remotas'] = pd.read_sql_query("""
                SELECT COALESCE(is_remote, 'Não informado') as is_remote, COUNT(*) as count 
                FROM vagas 
                GROUP BY COALESCE(is_remote, 'Não informado')
            """, conn)
            
            # Vagas por keyword
            stats['por_keyword'] = pd.read_sql_query("""
                SELECT COALESCE(keyword_busca, 'Não informado') as keyword_busca, COUNT(*) as count 
                FROM vagas 
                GROUP BY COALESCE(keyword_busca, 'Não informado')
                ORDER BY count DESC
            """, conn)
            
            # Vagas nas últimas 24h
            stats['ultimas_24h'] = pd.read_sql_query("""
                SELECT COUNT(*) as count 
                FROM vagas 
                WHERE data_coleta >= datetime('now', '-24 hours')
            """, conn).iloc[0]['count']
            
        except Exception as e:
            st.error(f"Erro ao obter estatísticas: {e}")
            stats = {'total': 0, 'ultimas_24h': 0}
        
        conn.close()
        return stats
    
    def executar_scraping_async(self, metodo):
        """Executa scraping em thread separada"""
        try:
            if metodo == "jobspy":
                return executar_scraping_jobspy()
            else:
                return executar_scraping_selenium()
        except Exception as e:
            st.error(f"Erro no scraping: {e}")
            return 0
    
    def verificar_ultimo_scraping(self):
        """Verifica quando foi o último scraping"""
        try:
            conn = self.conectar_db()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(data_coleta) FROM vagas")
            ultimo_scraping = cursor.fetchone()[0]
            conn.close()
            
            if ultimo_scraping:
                ultimo_datetime = datetime.strptime(ultimo_scraping, '%Y-%m-%d %H:%M:%S')
                return ultimo_datetime
            return None
        except Exception as e:
            return None
    
    def precisa_scraping_automatico(self):
        """Verifica se precisa executar scraping automático (a cada 2 horas)"""
        ultimo_scraping = self.verificar_ultimo_scraping()
        if not ultimo_scraping:
            return True
        
        agora = datetime.now()
        diferenca = agora - ultimo_scraping
        
        # Se passou mais de 2 horas (7200 segundos)
        return diferenca.total_seconds() > 7200



def renderizar_cards_vagas(df_vagas, cards_por_linha=2, app=None):
    """Renderiza vagas em formato de cards visuais"""
    
    if df_vagas.empty:
        st.warning("🚫 Nenhuma vaga para exibir em cards!")
        return
    
    # Organizar vagas em colunas
    vagas_list = df_vagas.to_dict('records')
    
    # Dividir vagas em grupos baseado no número de cards por linha
    for i in range(0, len(vagas_list), cards_por_linha):
        cols = st.columns(cards_por_linha)
        
        for j, vaga in enumerate(vagas_list[i:i+cards_por_linha]):
            with cols[j]:
                renderizar_card_individual(vaga, i+j, app)

def renderizar_card_individual(vaga, idx, app):
    """Renderiza um card individual de vaga"""
    
    # Truncar título e empresa para o card
    titulo_card = vaga['titulo'][:50] + "..." if len(vaga['titulo']) > 50 else vaga['titulo']
    empresa_card = vaga['empresa'][:30] + "..." if len(vaga['empresa']) > 30 else vaga['empresa']
    
    # Determinar cor baseada no site
    cores_site = {
        'linkedin': '#0077B5',
        'indeed': '#2557a7', 
        'glassdoor': '#0CAA41',
        'google': '#4285F4',
        'ziprecruiter': '#1c4a2c'
    }
    
    cor = cores_site.get(vaga['site_origem'], '#6c757d')
    
    # Card HTML
    card_html = f"""
    <div class="vaga-card" style="background: linear-gradient(135deg, {cor} 0%, {cor}AA 100%);">
        <div class="vaga-title">{titulo_card}</div>
        <div><strong>🏢 {empresa_card}</strong></div>
        <div>📍 {vaga['localizacao'][:40]}...</div>
        <div>🌐 {vaga['site_origem'].title()}</div>
        <div>💼 {vaga['job_type']}</div>
        <div>🏠 {'Remoto' if 'true' in str(vaga['is_remote']).lower() else 'Presencial'}</div>
        <div>💰 {vaga['salary_info'][:25]}...</div>
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)
    
    # Botões de ação
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Detalhes", key=f"detalhes_{idx}", type="secondary"):
            st.session_state[f'mostrar_detalhes_{idx}'] = True
    
    with col2:
        if vaga['link'] and vaga['link'] != '':
            st.markdown(f'<a href="{vaga["link"]}" target="_blank" class="link-button">🚀 Acessar</a>', 
                       unsafe_allow_html=True)
        else:
            st.button("🚫 Sem Link", disabled=True, key=f"no_link_{idx}")
    
    with col3:
        if st.button("🗑️ Deletar", key=f"delete_card_{idx}", type="secondary"):
            if app.deletar_vaga(vaga['id']):
                st.success("✅ Vaga deletada!")
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ Erro ao deletar!")
    
    # Modal de detalhes
    if st.session_state.get(f'mostrar_detalhes_{idx}', False):
        mostrar_modal_detalhes(vaga, idx)

def mostrar_modal_detalhes(vaga, idx):
    """Mostra modal com detalhes completos da vaga"""
    
    with st.expander(f"📋 Detalhes: {vaga['titulo'][:50]}...", expanded=True):
        
        # Botão para fechar modal
        if st.button("❌ Fechar", key=f"fechar_modal_{idx}"):
            st.session_state[f'mostrar_detalhes_{idx}'] = False
            st.rerun()
        
        # Informações principais em duas colunas
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **🎯 Área:**  
            {vaga['area_vaga']}
            
            **� Número de Candidatos:**  
            {vaga['numero_candidatos']}
            """)
        
        with col2:
            # Verificar se tem horário flexível na descrição
            descricao_texto = str(vaga['descricao']).lower()
            tem_horario_flexivel = any(termo in descricao_texto for termo in [
                'horário flexível', 'horario flexivel', 'flexible schedule', 
                'flexibilidade de horário', 'flexibilidade horário',
                'horários flexíveis', 'horarios flexiveis'
            ])
            
            st.markdown(f"""
            **⏰ Horário Flexível:**  
            {'✅ Sim' if tem_horario_flexivel else '❌ Não'}
            """)
        
        # Link da vaga
        st.markdown("---")
        if vaga['link'] and vaga['link'] != '':
            st.markdown("**🔗 Link da Vaga:**")
            col_link1, col_link2 = st.columns([4, 1])
            
            with col_link1:
                st.code(vaga['link'], language='text')
            
            with col_link2:
                st.markdown(f'<a href="{vaga["link"]}" target="_blank" class="link-button">🚀 Abrir</a>', 
                           unsafe_allow_html=True)
        else:
            st.warning("🚫 Link não disponível")
        
        # Descrição completa
        st.markdown("---")
        st.markdown("**📄 Descrição da Vaga:**")
        if vaga['descricao'] and vaga['descricao'] != 'Sem descrição':
            st.markdown(f"""
            <div style="background-color: #ffffff; padding: 20px; border-radius: 10px; border: 1px solid #e0e0e0; color: #000000; line-height: 1.6; max-height: 400px; overflow-y: auto;">
                {vaga['descricao'].replace(chr(10), '<br>')}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("📝 Descrição não disponível para esta vaga")

def main():
    app = StreamlitAppAvancado()
    
    # Inicializar session state completo
    if 'mostrar_confirmacao' not in st.session_state:
        st.session_state.mostrar_confirmacao = False
    
    if 'vaga_selecionada_idx' not in st.session_state:
        st.session_state.vaga_selecionada_idx = 0
    
    if 'horas_filtro' not in st.session_state:
        st.session_state.horas_filtro = None
    
    if 'limite_vagas' not in st.session_state:
        st.session_state.limite_vagas = 200
    
    if 'empresa_filtro' not in st.session_state:
        st.session_state.empresa_filtro = "Todas"
    
    if 'site_filtro' not in st.session_state:
        st.session_state.site_filtro = "Todos"
    
    if 'keyword_filtro' not in st.session_state:
        st.session_state.keyword_filtro = "Todas"
    
    if 'tipo_filtro' not in st.session_state:
        st.session_state.tipo_filtro = "Todos"
    
    if 'remoto_filtro' not in st.session_state:
        st.session_state.remoto_filtro = "Todos"
    
    if 'horario_flexivel_filtro' not in st.session_state:
        st.session_state.horario_flexivel_filtro = "Todos"
    
    if 'estados_selecionados' not in st.session_state:
        st.session_state.estados_selecionados = []
    
    if 'cidades_selecionadas' not in st.session_state:
        st.session_state.cidades_selecionadas = []
    
    if 'pergunta_inicial_feita' not in st.session_state:
        st.session_state.pergunta_inicial_feita = False
    
    if 'auto_scraping_ativo' not in st.session_state:
        st.session_state.auto_scraping_ativo = False
    
    if 'ultimo_auto_scraping' not in st.session_state:
        st.session_state.ultimo_auto_scraping = None
    
    # Header
    st.title("🔍 SearchVagas Dashboard Pro")
    st.markdown("**Sistema Avançado de Coleta de Vagas com Auto-Scraping**")
    
    # Status do auto-scraping no header
    if st.session_state.auto_scraping_ativo:
        ultimo_scraping = app.verificar_ultimo_scraping()
        if ultimo_scraping:
            proximo_scraping = ultimo_scraping + timedelta(hours=2)
            agora = datetime.now()
            
            if proximo_scraping > agora:
                tempo_restante = proximo_scraping - agora
                horas_restantes = tempo_restante.total_seconds() / 3600
                st.success(f"🤖 **Auto-scraping ATIVO** - Próximo em: {horas_restantes:.2f}h")
            else:
                st.warning("🤖 **Auto-scraping ATIVO** - Executando em breve...")
        else:
            st.info("🤖 **Auto-scraping ATIVO** - Aguardando primeiro ciclo...")
    else:
        st.info("⏸️ Auto-scraping desativado - Ative na barra lateral")
    
    # Pergunta inicial sobre scraping (apenas uma vez por sessão)
    if not st.session_state.pergunta_inicial_feita:
        st.markdown("---")
        st.markdown("### 🚀 Bem-vindo ao SearchVagas!")
        
        # Verificar se há dados no banco
        try:
            df_existente = app.obter_vagas_dataframe(limit=1)
            tem_dados = not df_existente.empty
        except:
            tem_dados = False
        
        if tem_dados:
            ultimo_scraping = app.verificar_ultimo_scraping()
            if ultimo_scraping:
                tempo_desde_ultimo = datetime.now() - ultimo_scraping
                horas_desde_ultimo = tempo_desde_ultimo.total_seconds() / 3600
                
                st.info(f"📊 Último scraping realizado há {horas_desde_ultimo:.1f} horas")
                
                if horas_desde_ultimo > 2:
                    st.warning("⏰ Recomendamos executar um novo scraping para dados atualizados!")
        else:
            st.warning("📭 Nenhum dado encontrado no banco. É necessário executar o scraping primeiro!")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🚀 Executar Scraping Agora", type="primary", use_container_width=True):
                with st.spinner("Executando scraping inicial..."):
                    resultado = app.executar_scraping_async("jobspy")
                    st.success(f"✅ {resultado} novas vagas coletadas!")
                    st.session_state.pergunta_inicial_feita = True
                    st.session_state.auto_scraping_ativo = True
                    st.session_state.ultimo_auto_scraping = datetime.now()
                    time.sleep(2)
                    st.rerun()
        
        with col2:
            if st.button("⏰ Ativar Auto-Scraping", type="secondary", use_container_width=True):
                st.session_state.auto_scraping_ativo = True
                st.session_state.pergunta_inicial_feita = True
                st.success("✅ Auto-scraping ativado! Executará a cada 2 horas.")
                time.sleep(1)
                st.rerun()
        
        with col3:
            if st.button("⏭️ Continuar sem Scraping", type="secondary", use_container_width=True):
                st.session_state.pergunta_inicial_feita = True
                st.info("💡 Você pode executar o scraping depois na barra lateral.")
                time.sleep(1)
                st.rerun()
        
        return  # Parar aqui até que o usuário faça uma escolha
    
    # Status do auto-scraping
    if st.session_state.auto_scraping_ativo:
        # Verificar se precisa executar scraping automático
        if app.precisa_scraping_automatico():
            # Mostrar aviso antes de executar
            with st.container():
                st.info("🔄 **Auto-scraping ativo** - Executando coleta automática de vagas...")
                
                with st.spinner("🔄 Executando scraping automático..."):
                    try:
                        resultado = app.executar_scraping_async("jobspy")
                        st.session_state.ultimo_auto_scraping = datetime.now()
                        st.success(f"🤖 Auto-scraping concluído: {resultado} novas vagas coletadas!")
                        
                        # Pequena pausa para mostrar o resultado
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro no auto-scraping: {e}")
                        st.warning("💡 Continuando com dados existentes...")
    
    # Sidebar
    st.sidebar.title("⚙️ Controles")
    
    # Status do Auto-Scraping
    st.sidebar.markdown("### 🤖 Auto-Scraping")
    
    if st.session_state.auto_scraping_ativo:
        st.sidebar.success("✅ Auto-scraping ATIVO")
        
        # Mostrar próximo scraping
        ultimo_scraping = app.verificar_ultimo_scraping()
        if ultimo_scraping:
            proximo_scraping = ultimo_scraping + timedelta(hours=2)
            agora = datetime.now()
            
            if proximo_scraping > agora:
                tempo_restante = proximo_scraping - agora
                horas_restantes = tempo_restante.total_seconds() / 3600
                st.sidebar.info(f"⏰ Próximo em: {horas_restantes:.1f}h")
            else:
                st.sidebar.warning("⏰ Executando em breve...")
        
        # Botão para desativar
        if st.sidebar.button("❌ Desativar Auto-Scraping"):
            st.session_state.auto_scraping_ativo = False
            st.sidebar.success("Auto-scraping desativado!")
            time.sleep(1)
            st.rerun()
    else:
        st.sidebar.warning("⏸️ Auto-scraping INATIVO")
        
        # Botão para ativar
        if st.sidebar.button("✅ Ativar Auto-Scraping"):
            st.session_state.auto_scraping_ativo = True
            st.sidebar.success("Auto-scraping ativado!")
            time.sleep(1)
            st.rerun()
    
    st.sidebar.markdown("---")
    
    # Botões de scraping
    st.sidebar.markdown("### 🚀 Executar Scraping")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🌟 JobsPy", help="Usar JobsPy (LinkedIn, Indeed, Google, Glassdoor)"):
            with st.spinner("Executando scraping com JobsPy..."):
                resultado = app.executar_scraping_async("jobspy")
                st.sidebar.success(f"✅ {resultado} novas vagas coletadas!")
                st.session_state.ultimo_auto_scraping = datetime.now()
                time.sleep(2)
                st.rerun()
    
    with col2:
        if st.button("🔧 Selenium", help="Usar Selenium (backup)"):
            with st.spinner("Executando scraping com Selenium..."):
                resultado = app.executar_scraping_async("selenium")
                st.sidebar.success(f"✅ {resultado} novas vagas coletadas!")
                st.session_state.ultimo_auto_scraping = datetime.now()
                time.sleep(2)
                st.rerun()
    
    # Filtros
    st.sidebar.markdown("### 🔽 Filtros")
    
    horas_filtro = st.sidebar.selectbox(
        "Vagas das últimas:",
        [None, 1, 2, 3, 4, 6, 12, 24, 48, 72, 168],
        format_func=lambda x: "Todas" if x is None else f"{x} horas",
        index=0,
        key="horas_filtro_select"
    )
    
    limite_vagas = st.sidebar.slider(
        "Limite de vagas", 
        10, 1000, 200, 10,
        key="limite_vagas_slider"
    )
    
    # Obter dados para filtros
    try:
        df_todos = app.obter_vagas_dataframe()
        
        if not df_todos.empty:
            # Obter estados e cidades disponíveis
            estados_cidades = app.extrair_estados_cidades()
            
            # Filtro de múltiplos estados
            estados_disponiveis = estados_cidades['estados']
            if estados_disponiveis:
                estados_selecionados = st.sidebar.multiselect(
                    "🗺️ Filtrar por Estados:",
                    options=estados_disponiveis,
                    default=[],
                    key="estados_filtro_select",
                    help="Selecione um ou mais estados"
                )
            else:
                estados_selecionados = []
            
            # Filtro de múltiplas cidades
            cidades_disponiveis = estados_cidades['cidades']
            if cidades_disponiveis:
                cidades_selecionadas = st.sidebar.multiselect(
                    "🏙️ Filtrar por Cidades:",
                    options=cidades_disponiveis,
                    default=[],
                    key="cidades_filtro_select",
                    help="Selecione uma ou mais cidades"
                )
            else:
                cidades_selecionadas = []
            
            # Filtros específicos - tratando valores None
            empresas_disponiveis = ['Todas'] + sorted([emp for emp in df_todos['empresa'].fillna('Não informado').unique().tolist() if emp])
            empresa_filtro = st.sidebar.selectbox(
                "Filtrar por empresa:", 
                empresas_disponiveis,
                key="empresa_filtro_select"
            )
            
            sites_disponiveis = ['Todos'] + sorted([site for site in df_todos['site_origem'].fillna('Não informado').unique().tolist() if site])
            site_filtro = st.sidebar.selectbox(
                "Filtrar por site:", 
                sites_disponiveis,
                key="site_filtro_select"
            )
            
            keywords_disponiveis = ['Todas'] + sorted([kw for kw in df_todos['keyword_busca'].fillna('Não informado').unique().tolist() if kw])
            keyword_filtro = st.sidebar.selectbox(
                "Filtrar por keyword:", 
                keywords_disponiveis,
                key="keyword_filtro_select"
            )
            
            tipos_disponiveis = ['Todos'] + sorted([tipo for tipo in df_todos['job_type'].fillna('Não informado').unique().tolist() if tipo])
            tipo_filtro = st.sidebar.selectbox(
                "Tipo de trabalho:", 
                tipos_disponiveis,
                key="tipo_filtro_select"
            )
            
            remoto_opcoes = ['Todos'] + sorted([rem for rem in df_todos['is_remote'].fillna('Não informado').unique().tolist() if rem])
            remoto_filtro = st.sidebar.selectbox(
                "Trabalho remoto:", 
                remoto_opcoes,
                key="remoto_filtro_select"
            )
            
            # Filtro por horário flexível
            horario_flexivel_filtro = st.sidebar.selectbox(
                "Horário flexível:", 
                ['Todos', 'True', 'False'],
                format_func=lambda x: 'Todos' if x == 'Todos' else ('✅ Sim' if x == 'True' else '❌ Não'),
                key="horario_flexivel_filtro_select"
            )
            
            # Aplicar filtros
            filtros = {
                'empresa': empresa_filtro,
                'site': site_filtro, 
                'keyword': keyword_filtro,
                'job_type': tipo_filtro,
                'is_remote': remoto_filtro,
                'horario_flexivel': horario_flexivel_filtro,
                'estados': estados_selecionados,
                'cidades': cidades_selecionadas
            }
        else:
            # Quando não há dados, definir valores padrão
            filtros = {}
            estados_selecionados = []
            cidades_selecionadas = []
            
        # Refresh automático
        auto_refresh = st.sidebar.checkbox(
            "🔄 Auto-refresh (2min)",
            key="auto_refresh_checkbox"
        )
        
        if auto_refresh:
            # Usar meta refresh do HTML em vez de sleep
            st.markdown('<meta http-equiv="refresh" content="120">', unsafe_allow_html=True)
        
        # Obter dados filtrados
        df_vagas = app.obter_vagas_dataframe(
            limit=limite_vagas, 
            horas_recentes=horas_filtro,
            filtros=filtros if not df_todos.empty else None
        )
        
        stats = app.obter_estatisticas()
        
        # Verificar se há dados
        if df_vagas.empty:
            st.warning("🚫 Nenhuma vaga encontrada. Execute o scraping primeiro!")
            
            # Botão para executar scraping diretamente
            col1, col2, col3 = st.columns(3)
            with col2:
                if st.button("🚀 Executar Scraping Agora", type="primary", use_container_width=True):
                    with st.spinner("Executando scraping..."):
                        resultado = app.executar_scraping_async("jobspy")
                        st.success(f"✅ {resultado} novas vagas coletadas!")
                        time.sleep(2)
                        st.rerun()
            return
        
        # Métricas principais
        st.markdown("### 📊 Métricas Principais")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total de Vagas", stats['total'])
        
        with col2:
            st.metric("Últimas 24h", stats['ultimas_24h'])
        
        with col3:
            st.metric("Sites Ativos", len(stats['por_site']))
        
        with col4:
            empresas_unicas = df_vagas['empresa'].nunique()
            st.metric("Empresas Únicas", empresas_unicas)
            
        with col5:
            vagas_filtradas = len(df_vagas)
            st.metric("Vagas Filtradas", vagas_filtradas)
        
        # Gráficos
        st.markdown("### 📈 Análises")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de vagas por site
            if not stats['por_site'].empty:
                fig_sites = px.pie(
                    stats['por_site'], 
                    values='count', 
                    names='site_origem',
                    title="Distribuição por Site",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig_sites, use_container_width=True)
        
        with col2:
            # Gráfico de vagas por keyword
            if not stats['por_keyword'].empty:
                fig_keywords = px.bar(
                    stats['por_keyword'], 
                    x='keyword_busca', 
                    y='count',
                    title="Vagas por Termo de Busca",
                    color='count',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig_keywords, use_container_width=True)
        
        # Cards de Vagas
        st.markdown("### 🎯 Vagas Encontradas")
        
        # Controles para cards
        col_card1, col_card2, col_card3, col_card4 = st.columns(4)
        
        with col_card1:
            cards_por_linha = st.selectbox(
                "Cards por linha:", 
                [1, 2, 3, 4], 
                index=1,
                key="cards_por_linha_select"
            )
        
        with col_card2:
            max_cards = st.slider(
                "Máximo de cards:", 
                5, 100, 50,
                key="max_cards_slider"
            )
        
        with col_card3:
            # Botão para exportar CSV
            if st.button("📄 Exportar CSV", type="secondary"):
                csv = df_vagas.to_csv(index=False)
                st.download_button(
                    label="💾 Download CSV",
                    data=csv,
                    file_name=f"vagas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col_card4:
            # Botão de deletar todos com confirmação
            if st.button("🗑️ Deletar Todas", type="secondary"):
                st.session_state.mostrar_confirmacao = True
        
        # Modal de confirmação para deletar todas
        if st.session_state.get('mostrar_confirmacao', False):
            st.markdown("---")
            st.warning("⚠️ **ATENÇÃO:** Esta ação irá deletar TODAS as vagas do banco de dados!")
            
            col_conf1, col_conf2, col_conf3 = st.columns(3)
            
            with col_conf1:
                if st.button("✅ Confirmar Exclusão", type="primary"):
                    if app.deletar_todas_vagas():
                        st.success("✅ Todas as vagas foram deletadas!")
                        st.session_state.mostrar_confirmacao = False
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Erro ao deletar vagas!")
            
            with col_conf2:
                if st.button("❌ Cancelar", type="secondary"):
                    st.session_state.mostrar_confirmacao = False
                    st.rerun()
            
            with col_conf3:
                st.empty()
        
        # Exibir métricas dos cards
        st.markdown("---")
        col_metrics1, col_metrics2, col_metrics3, col_metrics4 = st.columns(4)
        
        with col_metrics1:
            st.metric("📊 Total Exibido", min(len(df_vagas), max_cards))
        with col_metrics2:
            sites_unicos = df_vagas['site_origem'].nunique()
            st.metric("🌐 Sites", sites_unicos)
        with col_metrics3:
            empresas_unicas = df_vagas['empresa'].nunique()
            st.metric("🏢 Empresas", empresas_unicas)
        with col_metrics4:
            remotas = len(df_vagas[df_vagas['is_remote'].fillna('').astype(str).str.contains('True|true|Sim', case=False, na=False)])
            st.metric("🏠 Remotas", remotas)
        
        # Renderizar cards das vagas
        renderizar_cards_vagas(df_vagas.head(max_cards), cards_por_linha, app)
        
        # Informações do auto-scraping na parte inferior
        if st.session_state.auto_scraping_ativo:
            st.markdown("---")
            st.markdown("### 🤖 Status do Auto-Scraping")
            
            col_auto1, col_auto2, col_auto3 = st.columns(3)
            
            with col_auto1:
                st.info("✅ **Auto-scraping ATIVO**  \nExecuta a cada 2 horas automaticamente")
            
            with col_auto2:
                ultimo_scraping = app.verificar_ultimo_scraping()
                if ultimo_scraping:
                    tempo_desde = datetime.now() - ultimo_scraping
                    horas_desde = tempo_desde.total_seconds() / 3600
                    st.metric("⏰ Último Scraping", f"{horas_desde:.1f}h atrás")
                else:
                    st.metric("⏰ Último Scraping", "Nunca")
            
            with col_auto3:
                if ultimo_scraping:
                    proximo = ultimo_scraping + timedelta(hours=2)
                    if proximo > datetime.now():
                        tempo_para_proximo = proximo - datetime.now()
                        horas_para_proximo = tempo_para_proximo.total_seconds() / 3600
                        st.metric("⏭️ Próximo em", f"{horas_para_proximo:.1f}h")
                    else:
                        st.metric("⏭️ Próximo", "Executando...")
                else:
                    st.metric("⏭️ Próximo", "Em breve")
        
        # Verificação automática de scraping sem bloquear interface
        if st.session_state.auto_scraping_ativo:
            # Usar JavaScript para verificar a cada 2 minutos se precisa fazer scraping
            st.markdown("""
            <script>
            setTimeout(function() {
                if (window.location.reload) {
                    window.location.reload();
                }
            }, 120000); // 2 minutos
            </script>
            """, unsafe_allow_html=True)
        
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")
        st.info("💡 Tente executar o scraping primeiro")

if __name__ == "__main__":
    main()
