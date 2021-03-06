const { ObjectId } = require('mongodb')

class ModelEngineV2 {
    // module - 模块名
    //      'user_center', 'content_center', 'game_center'
    // table - 表名
    // dbMaster - 线上db连接
    // dbPort -  待迁移的库
    constructor(module, tableMaster, tablePort, dbMaster, dbPort, redisGetAsync, rdFlag, rwFlag) {
        this.module = module
        this.table = tablePort
        if (redisGetAsync) {
            this.collMaster = dbMaster.collection(tableMaster)
        }
        this.collPort = dbPort.collection(tablePort)
        this.redisGetAsync = redisGetAsync
        this.rdFlag = rdFlag
        this.rwFlag = rwFlag
    }

    find(query, options = {}) {
        const flag = this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster
        return coll.find(query, options)
    }

    findOne(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.findOne(query, options, callback)
    }

    countDocuments(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.countDocuments(query, options, callback)
    }

    aggregate(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.aggregate(query, options, callback)
    }

    async insertOne(query, options = null, callback = undefined) {
        if (!query["_id"]) {
            query["_id"] = ObjectId()
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._insertOne(this.collMaster, query, options, callback)
            await this._insertOne(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._insertOne(this.collPort, query, options, callback)
        }

        return await this._insertOne(this.collMaster, query, options, callback)
    }

    async _insertOne(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.insertOne(query, options, callback)
    }

    async insertMany(query, options = null, callback = undefined) {
        if (typeof query === "array") {
            query.forEach((o) => {
                o["_id"] = ObjectId()
            })
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._insertMany(this.collMaster, query, options, callback)
            await this._insertMany(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._insertMany(this.collPort, query, options, callback)
        }

        return await this._insertMany(this.collMaster, query, options, callback)
    }

    async _insertMany(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.insertMany(query, options, callback)
    }

    async deleteOne(query, options = null, callback = undefined) {
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._deleteOne(this.collMaster, query, options, callback)
            await this._deleteOne(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._deleteOne(this.collPort, query, options, callback)
        }

        return await this._deleteOne(this.collMaster, query, options, callback)
    }

    async _deleteOne(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.deleteOne(query, options, callback)
    }

    async deleteMany(query, options = null, callback = undefined) {
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._deleteMany(this.collMaster, query, options, callback)
            await this._deleteMany(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._deleteMany(this.collPort, query, options, callback)
        }

        return await this._deleteMany(this.collMaster, query, options, callback)
    }

    async _deleteMany(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.deleteMany(query, options, callback)
    }

    async insert(document, options = null, callback = undefined) {
        if (typeof document === "array") {
            document.forEach((o) => {
                o["_id"] = ObjectId()
            })
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._insert(this.collMaster, document, options, callback)
            await this._insert(this.collPort, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._insert(this.collPort, document, options, callback)
        }

        return await this._insert(this.collMaster, document, options, callback)
    }

    async _insert(coll, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.insert(document, options, callback)
    }

    async update(filter, document, options = null, callback = undefined) {
        if (!filter['_id']) {
            if (document['$setOnInsert'] && !document['$setOnInsert']['_id']) {
                document['$setOnInsert']['_id'] = ObjectId()
            } else if (!document['$setOnInsert']) {
                document['$setOnInsert'] = { _id: ObjectId() }
            }
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._update(this.collMaster, filter, document, options, callback)
            await this._update(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._update(this.collPort, filter, document, options, callback)
        }

        return await this._update(this.collMaster, filter, document, options, callback)
    }

    async _update(coll, filter, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.update(filter, document, options, callback)
    }

    async updateOne(filter, document, options = null, callback = undefined) {
        if (!filter['_id']) {
            if (document['$setOnInsert'] && !document['$setOnInsert']['_id']) {
                document['$setOnInsert']['_id'] = ObjectId()
            } else if (!document['$setOnInsert']) {
                document['$setOnInsert'] = { _id: ObjectId() }
            }
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._updateOne(this.collMaster, filter, document, options, callback)
            await this._updateOne(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._updateOne(this.collPort, filter, document, options, callback)
        }

        return await this._updateOne(this.collMaster, filter, document, options, callback)
    }

    async _updateOne(coll, filter, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.updateOne(filter, document, options, callback)
    }

    async updateMany(filter, document, options = null, callback = undefined) {
        if (!filter['_id']) {
            if (document['$setOnInsert'] && !document['$setOnInsert']['_id']) {
                document['$setOnInsert']['_id'] = ObjectId()
            } else if (!document['$setOnInsert']) {
                document['$setOnInsert'] = { _id: ObjectId() }
            }
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._updateMany(this.collMaster, filter, document, options, callback)
            await this._updateMany(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._updateMany(this.collPort, filter, document, options, callback)
        }

        return await this._updateMany(this.collMaster, filter, document, options, callback)
    }

    async _updateMany(coll, filter, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.updateMany(filter, document, options, callback)
    }

    async findOneAndUpdate(filter, document, options = null, callback = undefined) {
        if (!filter['_id']) {
            if (document['$setOnInsert'] && !document['$setOnInsert']['_id']) {
                document['$setOnInsert']['_id'] = ObjectId()
            } else if (!document['$setOnInsert']) {
                document['$setOnInsert'] = { _id: ObjectId() }
            }
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = await this._findOneAndUpdate(this.collMaster, filter, document, options, callback)
            await this._findOneAndUpdate(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return await this._findOneAndUpdate(this.collPort, filter, document, options, callback)
        }

        return await this._findOneAndUpdate(this.collMaster, filter, document, options, callback)
    }

    async _findOneAndUpdate(coll, filter, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.findOneAndUpdate(filter, document, options, callback)
    }

    // 读标志
    flagForRead() {
        return this.rdFlag || null
    }

    // 写标志
    async flagForWrite() {
        if (this.rwFlag) {
            return this.rwFlag
        }
        if (!this.redisGetAsync) {
            return "new_gcp"
        }
        return await this.redisGetAsync(this.rwKey())
    }

    // 是否双写
    isDual(flag) {
        return flag == 'dual'
    }
    // 是否写新库
    isPort(flag) {
        return flag == 'new_gcp'
    }

    // 读key
    rdKey() { return 'switch:' + this.module + ':rd:' + this.table }
    // 写key
    rwKey() { return 'switch:' + this.module + ':rw:' + this.table }

    // debug
    async debug() {
        const flagWrite = await this.flagForWrite()
        const flagRead = this.flagForRead()

        const obj = {
            'key_read': this.rdKey(),
            'flag_read': this.isPort(flagRead) ? 'atlas' : 'new_gcp',
            'key_write': this.rwKey(),
            'flag_write': this.isDual(flagWrite) ? 'dual' : this.isPort(flagWrite) ? 'new_gcp' : 'atlas',
            'table': this.table,
            'module': this.module,
        }

        console.log(obj)
        return obj
    }
}

async function getCollectionAsync(module, tableMaster, tablePort, dbMaster, dbPort, redisGetAsync) {
    const rdFlag = await redisGetAsync(`switch:${module}:rd:${tablePort}}`)
    return new ModelEngineV2(module, tableMaster, tablePort, dbMaster, dbPort, redisGetAsync, rdFlag)
}

async function getMultiCollectionsAsync(module, dbMaster, clientPort, tables, redisGetAsync = null, rdFlag = null, rwFlag = null) {
  const dbPort = clientPort.db(module)
  return await Promise.all(tables.map(async(v) => {
    const tablePort = v.length == 1 ? v[0] : v[1]
    const key = `switch:${module}:rd:${tablePort}`
    const mRdFlag = await (async ()=>{
        if (rdFlag) {
            return rdFlag
        }
       return redisGetAsync ?  await redisGetAsync(key) : "new_gcp"
    })()
    return new ModelEngineV2(module, v[0], tablePort, dbMaster, dbPort, redisGetAsync, mRdFlag, rwFlag)
  }))
}

module.exports = {
    ModelEngineV2,
    getCollectionAsync,
    getMultiCollectionsAsync,
}