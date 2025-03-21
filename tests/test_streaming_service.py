import pytest
import asyncio
import time
from app.streaming.streaming_service import StreamingService, WatcherStats

@pytest.fixture
async def streaming_service():
    service = StreamingService()
    await service.start()
    yield service
    await service.stop()

@pytest.mark.asyncio
async def test_register_watcher(streaming_service):
    """Test l'enregistrement d'un nouveau watcher."""
    watcher_id = "test_watcher_1"
    channel = "test_channel"
    
    streaming_service.register_watcher(watcher_id, channel)
    
    # Vérifier que le watcher a été enregistré
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats is not None
    assert watcher_stats.current_channel == channel
    assert watcher_stats.bytes_transferred == 0
    assert watcher_stats.playlist_requests == 0
    
    # Vérifier les statistiques de la chaîne
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_watchers'] == 1
    assert channel_stats['peak_watchers'] == 1

@pytest.mark.asyncio
async def test_register_duplicate_watcher(streaming_service):
    """Test l'enregistrement d'un watcher déjà existant."""
    watcher_id = "test_watcher_2"
    channel = "test_channel"
    
    # Premier enregistrement
    streaming_service.register_watcher(watcher_id, channel)
    
    # Deuxième enregistrement du même watcher
    streaming_service.register_watcher(watcher_id, channel)
    
    # Vérifier que les statistiques n'ont pas changé
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_watchers'] == 1

@pytest.mark.asyncio
async def test_update_watcher_activity(streaming_service):
    """Test la mise à jour de l'activité d'un watcher."""
    watcher_id = "test_watcher_3"
    channel = "test_channel"
    
    streaming_service.register_watcher(watcher_id, channel)
    
    # Simuler un transfert de données
    bytes_transferred = 1024
    await streaming_service.update_watcher_activity(watcher_id, bytes_transferred)
    
    # Vérifier les statistiques mises à jour
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats.bytes_transferred == bytes_transferred
    
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_bytes'] == bytes_transferred

@pytest.mark.asyncio
async def test_change_channel(streaming_service):
    """Test le changement de chaîne d'un watcher."""
    watcher_id = "test_watcher_4"
    old_channel = "channel_1"
    new_channel = "channel_2"
    
    # Enregistrer le watcher sur la première chaîne
    streaming_service.register_watcher(watcher_id, old_channel)
    
    # Changer de chaîne
    await streaming_service.change_channel(watcher_id, new_channel)
    
    # Vérifier les statistiques
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats.current_channel == new_channel
    
    old_channel_stats = streaming_service.get_channel_stats(old_channel)
    assert old_channel_stats['total_watchers'] == 0
    
    new_channel_stats = streaming_service.get_channel_stats(new_channel)
    assert new_channel_stats['total_watchers'] == 1

@pytest.mark.asyncio
async def test_remove_watcher(streaming_service):
    """Test la suppression d'un watcher."""
    watcher_id = "test_watcher_5"
    channel = "test_channel"
    
    streaming_service.register_watcher(watcher_id, channel)
    
    # Supprimer le watcher
    await streaming_service.remove_watcher(watcher_id)
    
    # Vérifier que le watcher a été supprimé
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats is None
    
    # Vérifier les statistiques de la chaîne
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_watchers'] == 0

@pytest.mark.asyncio
async def test_inactive_watcher_cleanup(streaming_service):
    """Test le nettoyage des watchers inactifs."""
    watcher_id = "test_watcher_6"
    channel = "test_channel"
    
    streaming_service.register_watcher(watcher_id, channel)
    
    # Simuler une inactivité prolongée
    await asyncio.sleep(streaming_service._inactivity_threshold + 1)
    
    # Attendre un cycle de nettoyage
    await asyncio.sleep(streaming_service._cleanup_interval + 1)
    
    # Vérifier que le watcher a été supprimé
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats is None

@pytest.mark.asyncio
async def test_multiple_watchers(streaming_service):
    """Test la gestion de plusieurs watchers sur la même chaîne."""
    channel = "test_channel"
    watcher_ids = [f"test_watcher_{i}" for i in range(3)]
    
    # Enregistrer plusieurs watchers
    for watcher_id in watcher_ids:
        streaming_service.register_watcher(watcher_id, channel)
    
    # Vérifier les statistiques de la chaîne
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_watchers'] == len(watcher_ids)
    assert channel_stats['peak_watchers'] == len(watcher_ids)
    
    # Supprimer un watcher
    await streaming_service.remove_watcher(watcher_ids[0])
    
    # Vérifier les statistiques mises à jour
    channel_stats = streaming_service.get_channel_stats(channel)
    assert channel_stats['total_watchers'] == len(watcher_ids) - 1

@pytest.mark.asyncio
async def test_concurrent_operations(streaming_service):
    """Test les opérations concurrentes sur les watchers."""
    watcher_id = "test_watcher_7"
    channel = "test_channel"
    
    # Créer plusieurs tâches concurrentes
    tasks = []
    for _ in range(5):
        tasks.append(streaming_service.update_watcher_activity(watcher_id, 1024))
    
    # Exécuter les tâches en parallèle
    await asyncio.gather(*tasks)
    
    # Vérifier que les statistiques sont cohérentes
    watcher_stats = streaming_service.get_watcher_stats(watcher_id)
    assert watcher_stats.bytes_transferred == 5 * 1024

@pytest.mark.asyncio
async def test_get_active_watchers_count(streaming_service):
    """Test le comptage des watchers actifs."""
    channel = "test_channel"
    watcher_ids = [f"test_watcher_{i}" for i in range(3)]
    
    # Enregistrer plusieurs watchers
    for watcher_id in watcher_ids:
        streaming_service.register_watcher(watcher_id, channel)
    
    # Vérifier le comptage
    count = streaming_service.get_active_watchers_count(channel)
    assert count == len(watcher_ids)
    
    # Supprimer un watcher et vérifier le comptage
    await streaming_service.remove_watcher(watcher_ids[0])
    count = streaming_service.get_active_watchers_count(channel)
    assert count == len(watcher_ids) - 1 