import threading
import time
import logging
import mariadb
import os
from database import DB_CONFIG, get_db_cursor

logger = logging.getLogger(__name__)

# Global dictionary to signal threads WITHIN THE SAME PROCESS to stop
_stop_signals = {}

def get_db():
    try:
        return mariadb.connect(**DB_CONFIG)
    except:
        return None

def signal_stop(task_id):
    """Signals a task to stop (both in-memory and in DB)."""
    # 1. In-memory (for current process)
    _stop_signals[task_id] = True
    
    # 2. In DB (for cross-process or UI visual feedback)
    try:
        with get_db_cursor() as cursor:
            cursor.execute("UPDATE sys_active_tasks SET requested_stop = 1, status = 'STOPPING' WHERE task_id = %s", (task_id,))
    except Exception as e:
        logger.error(f"Error signaling stop in DB for {task_id}: {e}")
        
    logger.info(f"Signal STOP sent to task {task_id}")

def should_stop(task_id):
    """
    Checks if a task should stop. 
    Checks both local memory and DB to allow cross-process termination.
    """
    # Check memory first (fastest)
    if _stop_signals.get(task_id, False):
        return True
        
    # Check DB (needed for CLI vs WEB communication)
    try:
        # We don't use get_db_cursor here because this is called frequently in loops
        # and we want to avoid opening/closing too many connections or use a lighter check.
        # But if we don't have a shared state, DB is the only way.
        conn = get_db()
        if not conn: return False
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT requested_stop FROM sys_active_tasks WHERE task_id = %s", (task_id,))
            row = cursor.fetchone()
            if row and row[0]:
                _stop_signals[task_id] = True # Cache it
                return True
        finally:
            conn.close()
    except:
        pass
        
    return False

def clear_stop_signal(task_id):
    """Clears the stop signal for a task."""
    if task_id in _stop_signals:
        del _stop_signals[task_id]
        
    try:
        with get_db_cursor() as cursor:
            cursor.execute("UPDATE sys_active_tasks SET requested_stop = 0 WHERE task_id = %s", (task_id,))
    except:
        pass

def register_thread(task_id, description, process_name="Tarea de Fondo", priority=5, parent_id=None, source_origin='WEB', enterprise_id=None):
    """Registers a thread for monitoring in the DB."""
    # Attempt to get enterprise_id from g if not provided (for web requests)
    ent_id = enterprise_id
    if ent_id is None:
        try:
            from flask import g
            ent_id = g.user.get('enterprise_id') if g.user else None
        except:
            pass

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO sys_active_tasks 
                (task_id, enterprise_id, process_name, description, priority, parent_id, thread_id, start_time, os_pid, source_type, source_origin, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    enterprise_id=%s, process_name=%s, description=%s, priority=%s, parent_id=%s, thread_id=%s, 
                    start_time=%s, os_pid=%s, source_type=%s, source_origin=%s, status=%s, last_heartbeat=NOW()
            """, (task_id, ent_id, process_name, description, priority, parent_id, threading.get_ident(), time.time(), os.getpid(), 'DB_TASK', source_origin, 'RUNNING',
                  ent_id, process_name, description, priority, parent_id, threading.get_ident(), time.time(), os.getpid(), 'DB_TASK', source_origin, 'RUNNING'))
    except Exception as e:
        logger.error(f"Error registering task in DB: {e}")

def unregister_thread(task_id):
    """Unregisters a thread from the DB."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("DELETE FROM sys_active_tasks WHERE task_id = %s", (task_id,))
    except Exception as e:
        logger.error(f"Error unregistering task in DB: {e}")

def get_active_tasks(enterprise_id=None):
    """Returns a list of active tasks from the DB."""
    tasks = {}
    ent_id = enterprise_id
    if ent_id is None:
        try:
            from flask import g
            ent_id = g.user.get('enterprise_id') if g.user else None
        except:
            pass

    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Cleanup stale tasks
            cursor.execute("DELETE FROM sys_active_tasks WHERE last_heartbeat < SUBTIME(NOW(), '00:02:00')")
            
            if ent_id is not None:
                cursor.execute("SELECT * FROM sys_active_tasks WHERE enterprise_id = %s", (ent_id,))
            else:
                cursor.execute("SELECT * FROM sys_active_tasks")
                
            for row in cursor.fetchall():
                tasks[row['task_id']] = row
    except Exception as e:
        logger.error(f"Error getting tasks from DB: {e}")
    return tasks

def update_heartbeat(task_id, status=None):
    """Updates the heartbeat and optionally the status of a task."""
    try:
        with get_db_cursor() as cursor:
            if status:
                cursor.execute("UPDATE sys_active_tasks SET last_heartbeat = NOW(), status = %s WHERE task_id = %s", (status, task_id))
            else:
                cursor.execute("UPDATE sys_active_tasks SET last_heartbeat = NOW() WHERE task_id = %s", (task_id,))
    except:
        pass

