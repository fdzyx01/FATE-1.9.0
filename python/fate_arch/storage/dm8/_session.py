import traceback

import dmPython

from fate_arch.storage import StorageSessionBase, StorageEngine, DM8StoreType
from fate_arch.abc import AddressABC
from fate_arch.common.address import DM8Address

class StorageSession(StorageSessionBase):
    def __init__(self, session_id, options=None):
        super(StorageSession, self).__init__(session_id=session_id, engine=StorageEngine.DM8)
        self._db_con = {}

    def table(self, name, namespace, address: AddressABC, partitions,
              store_type: DM8StoreType = DM8StoreType.OLTP, options=None, **kwargs):

        if isinstance(address, DM8Address):
            from fate_arch.storage.dm8._table import StorageTable
            address_key = DM8Address(user=None,
                                     passwd=None,
                                     host=address.host,
                                     port=address.port,
                                     name=None)

            if address_key in self._db_con:
                con, cur = self._db_con[address_key]
            else:
                con = dmPython.connect(user=address.user,
                                       password=address.passwd,
                                       server=address.host,
                                       port=address.port)
                cur = con.cursor()
                self._db_con[address_key] = (con, cur)

            return StorageTable(cur=cur, con=con, address=address, name=name, namespace=namespace,
                                store_type=store_type, partitions=partitions, options=options)

        raise NotImplementedError(f"address type {type(address)} not supported with eggroll storage")

    def cleanup(self, name, namespace):
        pass

    def stop(self):
        try:
            for key, val in self._db_con.items():
                con = val[0]
                cur = val[1]
                cur.close()
                con.close()
        except Exception as e:
            traceback.print_exc()

    def kill(self):
        return self.stop()