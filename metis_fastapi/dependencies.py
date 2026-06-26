# dependency file
from modules.RedisProxy import RedisProxy
import socket, os

_proxy_by_db = {}

# get redis proxy instance
def get_redis_proxy(db: int = 0) -> RedisProxy:
  if db in _proxy_by_db:
    return _proxy_by_db[db]

  redis_host = os.getenv("REDIS_SERVER_HOST", "localhost")
  redis_port = int(os.getenv("REDIS_SERVER_PORT", 6379)) 
  print(f"Connecting to Redis server at {redis_host}:{redis_port}, db={db}")
  # check availability of redis server
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    result = sock.connect_ex((redis_host, redis_port))
  except Exception as e:
    raise ConnectionError(f"Error connecting to Redis server at {redis_host}:{redis_port}: {e}")
  # close the socket after checking
  sock.close()
  
  aProxy = RedisProxy()
  aProxy.connect(redis_host, redis_port, db)
  _proxy_by_db[db] = aProxy
  return aProxy