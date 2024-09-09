#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import numpy as np
from federatedml.secureprotol.spdz.tensor.fixedpoint_numpy import FixedPointTensor
from federatedml.secureprotol.spdz import SPDZ
from fate_arch.session import Session

s = Session()
# on guest side
guest_party_id = 10000
host_party_id = 10001
guest_proxy_ip = "192.168.0.2"  # Generally, it is your current machine IP
federation_id = "spdz_demo"     # choose a common federation id (this should be same in both site)
session_id = "_".join([federation_id, "guest", str(guest_party_id)])
s.init_computing(session_id)
s.init_federation(federation_id,
                  runtime_conf={
                      "local": {"role": "guest", "party_id": guest_party_id},
                      "role": {"guest": [guest_party_id], "host": [host_party_id]},
                  },
                  service_conf={"host": guest_proxy_ip, "port": 9370})
s.as_global()
partys = s.parties.all_parties
# [Party(role=guest, party_id=10000), Party(role=host, party_id=10001)]


# on guest side(assuming local Party is partys[0]):
data = np.array([[1, 2, 3], [4, 5, 6]])
with SPDZ() as spdz:
    x = FixedPointTensor.from_source("x", data)
    y = FixedPointTensor.from_source("y", partys[1])

    z = (x + y).get()
    t = (x - y).get()
    print(z)
    print(t)
