import subprocess
from fastapi import APIRouter

router = APIRouter(
   prefix="/syscmd",
   tags=["syscmd"]
)

@router.get('/')
async def root() : 
   pass

@router.get('/exec/{cmd:path}')
async def exec(cmd: str):
   try:
      completed = subprocess.run(
         cmd,
         shell=True,
         text=True,
         stdout=subprocess.PIPE,
         stderr=subprocess.PIPE,
      )
      ret_out = (completed.stdout or "").strip()
      ret_err = (completed.stderr or "").strip()
      message = ret_out if ret_out else ret_err
      return {
         "message": message,
         "stdout": ret_out,
         "stderr": ret_err,
         "returncode": int(completed.returncode),
         "success": completed.returncode == 0,
      }
   except Exception as e:
      ret = "Error in subprocess.run(" + cmd + ") : " + str(e)
      return {
         "message": ret,
         "stdout": "",
         "stderr": ret,
         "returncode": -1,
         "success": False,
      }

@router.get('/exec/')
async def exec():
   return {"message": "No command"}
