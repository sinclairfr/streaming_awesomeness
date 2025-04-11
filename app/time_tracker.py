"""
CLASSE DUMMY POUR LA COMPATIBILITÉ
CETTE CLASSE EST MAINTENANT VIDE CAR TOUTE LA LOGIQUE A ÉTÉ DÉPLACÉE DANS CLIENT_MONITOR
"""

class TimeTracker:
    """Version factice pour compatibilité uniquement"""
    
    def __init__(self, stats_collector):
        """Constructor vide"""
        self.stats_collector = stats_collector
        
    def record_activity(self, *args, **kwargs):
        """Méthode vide pour compatibilité"""
        pass
        
    def get_active_watchers(self, *args, **kwargs):
        """Méthode vide pour compatibilité - retourne toujours un set vide"""
        return set()