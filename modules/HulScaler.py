import re
import subprocess
import time

import numpy as np
from modules.BaseScaler import BaseScaler


class HulScaler(BaseScaler):
    CommandPath = ""
    VersionCommand = "get_version"

    def __init__(self, ip: str):
        super(HulScaler, self).__init__()
        self._suspend = True
        self._types = {
            "0xc480": {"name": "StrHrTdc Base", "numCh": 32, "numBit": [32] * 32},
            "0x60c4": {"name": "StrLrTdc", "numCh": 129, "numBit": [32] * 129},
            "0xf000": {"name": "Mikumari", "numCh": 66, "numBit": [32] * 66},
        }
        self._dataCmd = {
            "0xc480": [
                [
                    "read_mzn_scr {1} up {0} 0",
                    "od -An -j80 -v -t u4 -w4 {0}",
                    "od -An -j12 -N4 -v -t u4 -w4 {0}",
                ],
            ],
            "0x60c4": [
                [
                    "read_scr {1} {0} 0",
                    "od -An -j80 -v -t u4 -w4 {0}",
                    "od -An -j12 -N4 -v -t u4 -w4 {0}",
                ],
            ],
            "0xf000": [
                [
                    "read_scr {1} {0} 0",
                    "od -An -j80 -v -t u4 -w4 {0}",
                    "od -An -j12 -N4 -v -t u4 -w4 {0}",
                ]
            ],
        }
        self._address = ip
        self._type = ""
        self.getVersion(ip)
        self._done = False

    def suspend(self):
        self._suspend = True

    def resume(self):
        self._suspend = False

    def stop(self):
        self._done = True

    def isValid(self):
        isValid = True
        print(self._info)
        for info in self._info.values():
            print(info["isValid"])
            isValid *= info["isValid"]
        return isValid

    def loopGetData(self, nLoop=0):
        iLoop = -1
        lastModified = 0
        period = 1  # second
        checkStep = 0.1
        while iLoop < nLoop:
            if (self._suspend) :
                time.sleep(period)
                continue
            if time.time() - lastModified < period:
                time.sleep(checkStep)
                continue
            if nLoop > 0 :
                iLoop += 1
            lastModified = time.time()
            self.getData()
#            if self._done:
#                break


    def getData(self):
        print(self._type)
        print(self._dataCmd[self._type])
        print(self._data)
        self._lastData = self._data
        for idx, fmts in enumerate(self._dataCmd[self._type]):
            cmd = []
            sid = "{}-{}".format(self._address, idx)
            for fmt in fmts:
                cmd.append(
                    fmt.format(
                        ".scaler.{}-{}.txt".format(self._address, idx),
                        self._address,
                    )
                )
            cmd = " && ".join(
                cmd,
            )
            cmd = HulScaler.CommandPath + "" + cmd + ""
            print(cmd)
            lines = (
                (subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True))
                .communicate()[0]
                .decode("utf-8")
                .split("\n")
            )
            values = []
            for line in lines:
                if (not len(line)) or line[0] == "*" or line[0] == "#":
                    continue
                values.append(int(line))
            hbfn = values.pop(-1)
            timestamp = time.time()
            if len(values) == self._info[sid]["numCh"]:
                print("good")
            else:
                print("bad")

            diffValues = [0.] * self._info[sid]["numCh"]
            diffHbfn = 0
            diffTs = 0
            if len(self._lastData) > 0 and sid in self._lastData:
                lastValues = self._lastData[sid]["data"]
                lastHbfn = self._lastData[sid]["hbfn"]
                lastUpdate = self._lastData[sid]["ts"]
                for i, val in enumerate(lastValues):
                    diffValues[i] = values[i] - val
                    diffValues[i] += (
                        2 ** self._info[sid]["numBit"][i] if diffValues[i] < 0 else 0
                    )
                diffHbfn = hbfn - lastHbfn
                diffHbfn += (2**24) if diffHbfn < 0 else 0

                diffTs = float(timestamp) - float(lastUpdate)
            self._data[sid] = {
                "data": values,
                "hbfn": hbfn,
                "ts": timestamp,
                "diff": diffValues,
                "diffHbfn": diffHbfn,
                "diffTs": diffTs,
            }
        print(self._data)

    def getVersion(self, ip):
        cmd = HulScaler.CommandPath + HulScaler.VersionCommand
        lines = (
            (
                subprocess.Popen(
                    cmd + " " + ip, stdout=subprocess.PIPE, shell=True
                ).communicate()[0]
            )
            .decode("utf-8")
            .split("\n")
        )
        #        print(lines)
        info = {'address': self._address}
        for line in lines:
            if (not len(line)) or line[0] == "*" or line[0] == "#":
                continue
            result = re.match(r"FW ID +: (\S+)", line)
            if result:
                info["fwid"] = result.group(1)
                self._type = info["fwid"]
                continue
            result = re.match(r"FW version +: (\S+)", line)
            if result:
                info["fwver"] = result.group(1)
                continue

        if info["fwid"] in self._types:
            info["type"] = self._types[info["fwid"]]["name"]
            info["numCh"] = self._types[info["fwid"]]["numCh"]
            info["numBit"] = self._types[info["fwid"]]["numBit"]
            info["isValid"] = True
        else:
            info["type"] = "unknown"
            info["isValid"] = False
        if self._type not in self._dataCmd:
            sid = "{}-{}".format(info["address"], 0)
            self._info[sid] = info
            return
        for idx, fmt in enumerate(self._dataCmd[self._type]):
            sid = "{}-{}".format(info["address"], idx)
            self._info[sid] = info


def main():
    import sys

    if len(sys.argv) > 1:
        ip = sys.argv[1]
    else:
        ip = "192.168.2.169"
    HulScaler.CommandPath = "ssh ata03 "
    scaler = HulScaler(ip)
    print(scaler._info)
    scaler.loopGetData(2)
    print(scaler.getDataJson())


if __name__ == "__main__":
    main()
