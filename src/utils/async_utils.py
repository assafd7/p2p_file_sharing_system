import asyncio
from PyQt6.QtCore import QTimer

def defer_async_task(coro):
    """
    Safely schedule an async coroutine to run after the current Qt event loop iteration.
    This avoids re-entrancy issues with qasync and PyQt.
    """
    loop = asyncio.get_event_loop()
    def schedule():
        asyncio.create_task(coro)
    QTimer.singleShot(0, schedule) 