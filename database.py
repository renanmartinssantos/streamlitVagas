import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import hashlib

class DatabaseManager:
    def __init__(self, db_path="vagas_linkedin.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializa o banco de dados com a tabela de vagas"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Verificar se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vagas'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Criar tabela com todos os campos necessários
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vagas (
                    id TEXT PRIMARY KEY,
                    titulo TEXT NOT NULL,
                    empresa TEXT NOT NULL,
                    localizacao TEXT,
                    area_vaga TEXT,
                    descricao TEXT,
                    link TEXT NOT NULL,
                    data_postagem TEXT,
                    data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    numero_candidatos TEXT,
                    site_origem TEXT,
                    job_type TEXT,
                    is_remote TEXT,
                    salary_info TEXT,
                    keyword_busca TEXT,
                    estado TEXT,
                    local_busca TEXT
                )
            ''')
        else:
            # Verificar e adicionar colunas que possam estar faltando
            cursor.execute("PRAGMA table_info(vagas)")
            colunas_existentes = [info[1] for info in cursor.fetchall()]
            
            # Lista de colunas que devem existir
            colunas_necessarias = {
                'area_vaga': 'TEXT',
                'numero_candidatos': 'TEXT',
                'site_origem': 'TEXT',
                'job_type': 'TEXT',
                'is_remote': 'TEXT',
                'salary_info': 'TEXT',
                'estado': 'TEXT',
                'local_busca': 'TEXT'
            }
            
            # Adicionar colunas faltantes
            for coluna, tipo in colunas_necessarias.items():
                if coluna not in colunas_existentes:
                    try:
                        cursor.execute(f"ALTER TABLE vagas ADD COLUMN {coluna} {tipo}")
                        print(f"Adicionada coluna '{coluna}' à tabela vagas")
                    except sqlite3.Error as e:
                        print(f"Erro ao adicionar coluna {coluna}: {e}")
        
        conn.commit()
        conn.close()
    
    def gerar_id_vaga(self, titulo, empresa, link):
        """Gera um ID único para a vaga baseado no título, empresa e link"""
        texto_hash = f"{titulo}{empresa}{link}"
        return hashlib.md5(texto_hash.encode()).hexdigest()
    
    def vaga_existe(self, vaga_id):
        """Verifica se a vaga já existe no banco"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM vagas WHERE id = ?", (vaga_id,))
        existe = cursor.fetchone()[0] > 0
        
        conn.close()
        return existe
    
    def inserir_vaga(self, vaga_data):
        """Insere uma nova vaga no banco de dados"""
        vaga_id = self.gerar_id_vaga(vaga_data['titulo'], vaga_data['empresa'], vaga_data['link'])
        
        if self.vaga_existe(vaga_id):
            return False  # Vaga já existe
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Campos que devem estar presentes
        campos = [
            'titulo', 'empresa', 'localizacao', 'descricao', 'link', 'data_postagem', 
            'keyword_busca', 'area_vaga', 'numero_candidatos', 'site_origem', 
            'job_type', 'is_remote', 'salary_info', 'estado', 'local_busca'
        ]
        
        # Certifique-se de que todos os campos têm um valor
        valores = [vaga_id]  # Iniciar com ID
        placeholders = ["?"]  # Placeholder para ID
        
        for campo in campos:
            placeholders.append("?")
            valores.append(vaga_data.get(campo, 'Não informado'))
        
        # Construir a query dinamicamente
        query = f"INSERT INTO vagas (id, {', '.join(campos)}) VALUES ({', '.join(placeholders)})"
        
        cursor.execute(query, valores)
        
        conn.commit()
        conn.close()
        return True  # Vaga inserida com sucesso
    
    def obter_vagas(self, limit=None, horas_recentes=None, estados=None, horario_flexivel=None):
        """Obtém vagas do banco de dados com filtros diversos"""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT * FROM vagas"
        params = []
        conditions = []
        
        # Filtro por data de coleta
        if horas_recentes:
            data_limite = datetime.now() - timedelta(hours=horas_recentes)
            conditions.append("data_coleta >= ?")
            params.append(data_limite.strftime('%Y-%m-%d %H:%M:%S'))
        
        # Filtro por estado(s)
        if estados and isinstance(estados, list) and len(estados) > 0:
            # Criar placeholders para cada estado
            estados_placeholders = ', '.join(['?' for _ in estados])
            conditions.append(f"estado IN ({estados_placeholders})")
            params.extend(estados)
        
        # Filtro por horário flexível na descrição
        if horario_flexivel is not None:
            if horario_flexivel:
                # Se True, buscar vagas com "horário flexível" na descrição
                conditions.append("(descricao LIKE ? OR descricao LIKE ?)")
                params.append('%horário flexível%')
                params.append('%horario flexivel%')
            else:
                # Se False, buscar vagas sem "horário flexível" na descrição
                conditions.append("(descricao NOT LIKE ? AND descricao NOT LIKE ?)")
                params.append('%horário flexível%')
                params.append('%horario flexivel%')
        
        # Adicionar condições à query
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Ordenação e limite
        query += " ORDER BY data_coleta DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    
    def obter_estatisticas(self):
        """Obtém estatísticas das vagas"""
        conn = sqlite3.connect(self.db_path)
        
        # Total de vagas
        total_vagas = pd.read_sql_query("SELECT COUNT(*) as total FROM vagas", conn).iloc[0]['total']
        
        # Vagas por keyword
        vagas_por_keyword = pd.read_sql_query('''
            SELECT keyword_busca, COUNT(*) as quantidade 
            FROM vagas 
            GROUP BY keyword_busca
        ''', conn)
        
        # Vagas por empresa
        vagas_por_empresa = pd.read_sql_query('''
            SELECT empresa, COUNT(*) as quantidade 
            FROM vagas 
            GROUP BY empresa 
            ORDER BY quantidade DESC 
            LIMIT 10
        ''', conn)
        
        # Vagas por site
        vagas_por_site = pd.read_sql_query('''
            SELECT site_origem, COUNT(*) as quantidade 
            FROM vagas 
            GROUP BY site_origem 
            ORDER BY quantidade DESC
        ''', conn)
        
        # Vagas por estado
        vagas_por_estado = pd.read_sql_query('''
            SELECT estado, COUNT(*) as quantidade 
            FROM vagas 
            WHERE estado IS NOT NULL AND estado != 'Não informado'
            GROUP BY estado 
            ORDER BY quantidade DESC
        ''', conn)
        
        # Vagas das últimas 24 horas
        data_limite = datetime.now() - timedelta(hours=24)
        vagas_24h = pd.read_sql_query('''
            SELECT COUNT(*) as total_24h 
            FROM vagas 
            WHERE data_coleta >= ?
        ''', conn, params=[data_limite.strftime('%Y-%m-%d %H:%M:%S')]).iloc[0]['total_24h']
        
        conn.close()
        
        return {
            'total_vagas': total_vagas,
            'vagas_24h': vagas_24h,
            'vagas_por_keyword': vagas_por_keyword,
            'vagas_por_empresa': vagas_por_empresa,
            'vagas_por_site': vagas_por_site,
            'vagas_por_estado': vagas_por_estado
        }
