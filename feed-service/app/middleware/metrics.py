"""
Сбор метрик производительности
Для мониторинга и отладки
"""
import time
from datetime import datetime
from typing import Dict, Any
from collections import defaultdict, deque
import threading

class MetricsCollector:
    """
    Сбор метрик в памяти
    Для продакшена лучше использовать Prometheus или Yandex Monitoring
    """
    
    def __init__(self, max_timings: int = 1000):
        self._counters = defaultdict(int)
        self._gauges = defaultdict(float)
        self._timings = defaultdict(lambda: deque(maxlen=max_timings))
        self._lock = threading.Lock()
        self._start_time = datetime.utcnow()
    
    def increment(self, metric: str, value: int = 1) -> None:
        """Увеличить счетчик"""
        with self._lock:
            self._counters[metric] += value
    
    def gauge(self, metric: str, value: float) -> None:
        """Установить значение gauge (текущее значение)"""
        with self._lock:
            self._gauges[metric] = value
    
    def timing(self, metric: str, seconds: float) -> None:
        """Записать время выполнения в миллисекундах"""
        with self._lock:
            self._timings[metric].append(seconds * 1000)  # конвертируем в ms
    
    def timing_ms(self, metric: str, milliseconds: float) -> None:
        """Записать время выполнения в миллисекундах"""
        with self._lock:
            self._timings[metric].append(milliseconds)
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику по всем метрикам"""
        with self._lock:
            stats = {
                'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds(),
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
            }
            
            # Добавляем статистику по таймингам
            timings_stats = {}
            for metric, values in self._timings.items():
                if values:
                    values_list = list(values)
                    timings_stats[metric] = {
                        'count': len(values_list),
                        'avg': sum(values_list) / len(values_list),
                        'max': max(values_list),
                        'min': min(values_list),
                        'p95': self._percentile(values_list, 95),
                        'p99': self._percentile(values_list, 99)
                    }
            
            stats['timings'] = timings_stats
            return stats
    
    def _percentile(self, values, percentile):
        """Вычислить процентиль"""
        if not values:
            return 0
        sorted_values = sorted(values)
        k = (len(sorted_values) - 1) * percentile / 100
        f = int(k)
        c = int(k) + 1 if f < len(sorted_values) - 1 else f
        return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)
    
    def reset(self) -> None:
        """Сбросить все метрики (для тестов)"""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timings.clear()
            self._start_time = datetime.utcnow()


# Контекстный менеджер для измерения времени выполнения
class Timer:
    """Контекстный менеджер для измерения времени выполнения блока кода"""
    
    def __init__(self, metrics: MetricsCollector, metric_name: str):
        self.metrics = metrics
        self.metric_name = metric_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args):
        duration = time.time() - self.start_time
        self.metrics.timing(self.metric_name, duration)


# Глобальный экземпляр для использования во всем приложении
metrics = MetricsCollector()
