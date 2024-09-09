from fate_arch.storage import StorageEngine, DM8StoreType
from fate_arch.storage import StorageTableBase

class StorageTable(StorageTableBase):
    def __init__(
        self,
        cur,
        con,
        address=None,
        name: str = None,
        namespace: str = None,
        partitions: int = 1,
        store_type: DM8StoreType = DM8StoreType.OLTP,
        options=None,
    ):
        super(StorageTable, self).__init__(
            name=name,
            namespace=namespace,
            address=address,
            partitions=partitions,
            options=options,
            engine=StorageEngine.DM8,
            store_type=store_type,
        )
        self._cur = cur
        self._con = con

    def check_address(self):
        schema = self.meta.get_schema()
        if schema:
            if schema.get("sid") and schema.get("header"):
                sql = "SELECT {}, {} FROM {}".format(
                    schema.get("sid"), schema.get("header"), self._address.name
                )
            else:
                sql = "SELECT {} FROM {}".format(
                    schema.get("sid"), self._address.name
                )
            try:
                feature_data = self.execute(sql)
            except Exception as e:
                print(f"执行 SQL 失败: {e}")
                return False
            for feature in feature_data:
                if feature:
                    break
        return True

    @staticmethod
    def get_meta_header(feature_name_list):
        create_features = ""
        feature_list = []
        feature_size = "varchar(255)"
        for feature_name in feature_name_list:
            create_features += "{} {},".format(feature_name, feature_size)
            feature_list.append(feature_name)
        return create_features, feature_list

    def _count(self):
        sql = "select count(*) from {}".format(self._address.name)
        try:
            self._cur.execute(sql)
            # self.con.commit()
            ret = self._cur.fetchall()
            count = ret[0][0]
        except BaseException:
            count = 0
        return count

    def _collect(self, **kwargs) -> list:
        id_name, feature_name_list, _ = self._get_id_feature_name()
        id_feature_name = [id_name]
        id_feature_name.extend(feature_name_list)
        sql = "select {} from {}".format(",".join(id_feature_name), self._address.name)
        data = self.execute(sql)
        for line in data:
            feature_list = [str(feature) for feature in list(line[1:])]
            yield line[0], self.meta.get_id_delimiter().join(feature_list)

    def _put_all(self, kv_list, **kwargs):
        id_name, feature_name_list, id_delimiter = self._get_id_feature_name()
        feature_sql, feature_list = StorageTable.get_meta_header(feature_name_list)
        id_size = "varchar(100)"
        create_table = (
            "create table if not exists {}({} {} NOT NULL, {} PRIMARY KEY({}))".format(
                self._address.name, id_name, id_size, feature_sql, id_name
            )
        )
        self._cur.execute(create_table)
        sql = "REPLACE INTO {}({}, {})  VALUES".format(
            self._address.name, id_name, ",".join(feature_list)
        )
        for kv in kv_list:
            sql += '("{}", "{}"),'.format(kv[0], '", "'.join(kv[1].split(id_delimiter)))
        sql = ",".join(sql.split(",")[:-1]) + ";"
        self._cur.execute(sql)
        self._con.commit()

    def _destroy(self):
        sql = "drop table {}".format(self._address.name)
        self._cur.execute(sql)
        self._con.commit()

    def _save_as(self, address, name, namespace, partitions=None, **kwargs):
        sql = "create table {}.{} select * from {};".format(namespace, name, self._address.name)
        self._cur.execute(sql)
        self._con.commit()

    def execute(self, sql, select=True):
        self._cur.execute(sql)
        if select:
            while True:
                result = self._cur.fetchone()
                if result:
                    yield result
                else:
                    break
        else:
            result = self._cur.fetchall()
            return result

    def _get_id_feature_name(self):
        id = self.meta.get_schema().get("sid", "id")
        header = self.meta.get_schema().get("header", [])
        id_delimiter = self.meta.get_id_delimiter()
        if not header:
            feature_list = []
        elif isinstance(header, str):
            feature_list = header.split(id_delimiter)
        elif isinstance(header, list):
            feature_list = header
        else:
            feature_list = [header]
        if self.meta.get_extend_sid():
            id = feature_list[0]
            if len(feature_list) > 1:
                feature_list = feature_list[1:]
        return id, feature_list, id_delimiter
