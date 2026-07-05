import os
import threading
import time

from modules.HulScalerTask import HulScalerTask
from modules.HulScaler import HulScaler


class ScalerFactory:
    def __init__(self):
        self._info = {}
        self._data = {}
        self._keys = {}
        self._scalers = {}
        self._cache_lock = threading.Lock()
        self._cache_interval_sec = max(0.2, float(os.getenv("METIS_SCALER_CACHE_REFRESH_SEC", "1.0")))
        self._cache_stop = threading.Event()
        self._cache_thread = None

    def message(self, status: int, message: str, payload: dict = {}):
        ret = {'header': {'status': status, 'message':  message}}
        if len(payload):
            ret['payload'] = payload
        return ret

    def addHulScaler(self, id: str, series: str = ""):
        if id in self._scalers:
            return self.message(0, "already exists {}".format(id))
        aTask = HulScalerTask(id)
        print("addHulScaler")
        if not aTask.isValid():
            return self.message(-1, "scaler does not exists {}".format(id))
        self._keys[id] = []
        for infoKey in aTask.getInfo():
            self._info[infoKey] = aTask.getInfo()[infoKey]
            self._keys[id].append(infoKey)
        self._scalers[id] = aTask
        self._scalers[id].start()
        self._start_cache_worker_if_needed()
        return self.message(0, "successfully added {}".format(id))

    def _start_cache_worker_if_needed(self):
        if self._cache_thread is None or (not self._cache_thread.is_alive()):
            self._cache_stop.clear()
            self._cache_thread = threading.Thread(target=self._cache_worker, daemon=True)
            self._cache_thread.start()

    def _cache_worker(self):
        while not self._cache_stop.is_set():
            try:
                if len(self._scalers) > 0:
                    self.get_data()
            except Exception:
                pass
            self._cache_stop.wait(self._cache_interval_sec)

    def suspend(self):
        for id in self._scalers :
            self._scalers[id].suspend()

    def resume(self):
        for id in self._scalers :
            self._scalers[id].resume()
            

    def removeScaler(self, id: str):
        self._scalers[id].stop()
        for infoKey in self._keys[id]:
            self._info.pop(infoKey)
            self._data.pop(infoKey)
        self._keys.pop(id)
        self._scalers.pop(id)

    def get_info(self, id: str = ""):
        if len(id):
            if id in self._scalers:
                return self.message(0, "success", self._scalers[id].getInfo())
            else:
                return self.message(-1, "no such id {}".format(id))

        return self.message(0, "success", self._info)

    def get_data(self, id: str = ""):
        with self._cache_lock:
            return self._get_data_unlocked(id)

    def _get_data_unlocked(self, id: str = ""):
        if len(id):
            if id in self._scalers:
                payload = self._scalers[id].getData()
                for infoKey, data in payload.items():
                    self._data[infoKey] = data
                return self.message(0, "success for {}".format(id), payload)
            else:
                return self.message(-1, "no such id {}".format(id))

        for scaler in self._scalers.values():
            payload = scaler.getData()
            for infoKey, data in payload.items():
                self._data[infoKey] = data
        return self.message(0, "success", self._data)

    def get_data_cached(self, id: str = ""):
        with self._cache_lock:
            return self._get_data_cached_unlocked(id)

    def _get_data_cached_unlocked(self, id: str = ""):
        if len(id):
            if id not in self._scalers:
                return self.message(-1, "no such id {}".format(id))
            payload = {}
            for infoKey in self._keys.get(id, []):
                if infoKey in self._data:
                    payload[infoKey] = self._data[infoKey]
            return self.message(0, "success for {}".format(id), payload)

        return self.message(0, "success", self._data)


def main():
    HulScaler.CommandPath = "ssh ata03 "

    factory = ScalerFactory()
    factory.addScaler("192.168.2.169")


if __name__ == "__main__":
    main()
