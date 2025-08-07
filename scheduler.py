import schedule
import time
import threading
from datetime import datetime
import logging
from scraper import executar_scraping

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
        self.running = False
        self.thread = None
    
    def configurar_agendamentos(self):
        """Configura todos os agendamentos"""
        # Limpeza de agendamentos anteriores
        schedule.clear()
        
        # Scraping completo a cada 2 horas
        schedule.every(2).hours.do(self.executar_scraping_completo)
        
        # Verificação rápida a cada 10 minutos (estatísticas)
        schedule.every(10).minutes.do(self.verificacao_rapida)
        
        logger.info("Agendamentos configurados:")
        logger.info("- Scraping completo: a cada 2 horas")
        logger.info("- Verificação rápida: a cada 10 minutos")
    
    def executar_scraping_completo(self):
        """Executa o scraping completo"""
        logger.info("🔄 Iniciando scraping completo agendado...")
        try:
            novas_vagas = executar_scraping()
            logger.info(f"✅ Scraping completo concluído! {novas_vagas} novas vagas encontradas.")
        except Exception as e:
            logger.error(f"❌ Erro no scraping completo: {e}")
    
    def verificacao_rapida(self):
        """Verificação rápida do sistema"""
        logger.info("🔍 Executando verificação rápida...")
        # Aqui podemos adicionar verificações de saúde do sistema
        # Por exemplo, verificar se o banco está acessível
        try:
            from database import DatabaseManager
            db = DatabaseManager()
            stats = db.obter_estatisticas()
            logger.info(f"📊 Sistema ativo - Total de vagas: {stats['total_vagas']}")
        except Exception as e:
            logger.error(f"❌ Erro na verificação rápida: {e}")
    
    def iniciar(self):
        """Inicia o scheduler em thread separada"""
        if not self.running:
            self.configurar_agendamentos()
            self.running = True
            self.thread = threading.Thread(target=self._executar_loop, daemon=True)
            self.thread.start()
            logger.info("🚀 Scheduler iniciado com sucesso!")
    
    def parar(self):
        """Para o scheduler"""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("⏹️ Scheduler parado.")
    
    def _executar_loop(self):
        """Loop principal do scheduler"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Verificar a cada minuto
            except Exception as e:
                logger.error(f"Erro no loop do scheduler: {e}")
                time.sleep(60)
    
    def status(self):
        """Retorna o status atual do scheduler"""
        return {
            'running': self.running,
            'jobs': len(schedule.jobs),
            'next_run': str(schedule.next_run()) if schedule.jobs else "Nenhum job agendado"
        }

def executar_scraping_inicial():
    """Executa uma primeira coleta de vagas"""
    logger.info("🎯 Executando scraping inicial...")
    try:
        novas_vagas = executar_scraping()
        logger.info(f"✅ Scraping inicial concluído! {novas_vagas} vagas coletadas.")
        return novas_vagas
    except Exception as e:
        logger.error(f"❌ Erro no scraping inicial: {e}")
        return 0

if __name__ == "__main__":
    # Executar scraping inicial
    executar_scraping_inicial()
    
    # Iniciar scheduler
    scheduler = SchedulerManager()
    scheduler.iniciar()
    
    try:
        # Manter o programa rodando
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
        scheduler.parar()
