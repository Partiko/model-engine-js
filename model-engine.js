const { ObjectId } = require('mongodb')

class ModelEngine {
    // module - 模块名
    //      'user_center', 'content_center', 'game_center'
    // table - 表名
    // dbMaster - 线上db连接
    // dbPort -  待迁移的库
    constructor(module, tableMaster, tablePort, dbMaster, dbPort, redisGetAsync) {
        this.module = module
        this.table = tablePort
        // this.collMaster = dbMaster.collection(tableMaster)
        this.collPort = dbPort.collection(tablePort)
        this.redisGetAsync = redisGetAsync
    }

    async find(query, options = {}) {
        const flag = await this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster
        return coll.find(query, options)
    }

    async findOne(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = await this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.findOne(query, options, callback)
    }

    async countDocuments(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = await this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.countDocuments(query, options, callback)
    }

    async aggregate(query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        const flag = await this.flagForRead()
        const coll = this.isPort(flag) ? this.collPort : this.collMaster

        return coll.aggregate(query, options, callback)
    }

    async insertOne(query, options = null, callback = undefined) {
        if (!query["_id"]) {
            query["_id"] = ObjectId()
        }
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = this._insertOne(this.collMaster, query, options, callback)
            this._insertOne(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._insertOne(this.collPort, query, options, callback)
        }

        return this._insertOne(this.collMaster, query, options, callback)
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
            const result = this._insertMany(this.collMaster, query, options, callback)
            this._insertMany(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._insertMany(this.collPort, query, options, callback)
        }

        return this._insertMany(this.collMaster, query, options, callback)
    }

    async _insertMany(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.insertMany(query, options, callback)
    }

    async deleteOne(query, options = null, callback = undefined) {
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = this._deleteOne(this.collMaster, query, options, callback)
            this._deleteOne(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._deleteOne(this.collPort, query, options, callback)
        }

        return this._deleteOne(this.collMaster, query, options, callback)
    }

    async _deleteOne(coll, query, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.deleteOne(query, options, callback)
    }

    async deleteMany(query, options = null, callback = undefined) {
        const flag = await this.flagForWrite()
        if (this.isDual(flag)) {
            const result = this._deleteMany(this.collMaster, query, options, callback)
            this._deleteMany(this.collPort, query, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._deleteMany(this.collPort, query, options, callback)
        }

        return this._deleteMany(this.collMaster, query, options, callback)
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
            this._insert(this.collPort, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._insert(this.collPort, document, options, callback)
        }

        return this._insert(this.collMaster, document, options, callback)
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
            const result = this._update(this.collMaster, filter, document, options, callback)
            this._update(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._update(this.collPort, filter, document, options, callback)
        }

        return this._update(this.collMaster, filter, document, options, callback)
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
            const result = this._updateOne(this.collMaster, filter, document, options, callback)
            this._updateOne(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._updateOne(this.collPort, filter, document, options, callback)
        }

        return this._updateOne(this.collMaster, filter, document, options, callback)
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
            const result = this._updateMany(this.collMaster, filter, document, options, callback)
            this._updateMany(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._updateMany(this.collPort, filter, document, options, callback)
        }

        return this._updateMany(this.collMaster, filter, document, options, callback)
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
            const result = this._findOneAndUpdate(this.collMaster, filter, document, options, callback)
            this._findOneAndUpdate(this.collPort, filter, document, options, callback)

            return result
        }

        if (this.isPort(flag)) {
            return this._findOneAndUpdate(this.collPort, filter, document, options, callback)
        }

        return this._findOneAndUpdate(this.collMaster, filter, document, options, callback)
    }

    async _findOneAndUpdate(coll, filter, document, options = null, callback = undefined) {
        if (typeof options === 'function') (callback = options), (options = {});
        options = options || {};

        return coll.findOneAndUpdate(filter, document, options, callback)
    }

    // 读标志
    async flagForRead() { return 'gcp' }
    // 写标志
    async flagForWrite() { return 'gcp' }

    // 是否双写
    isDual(flag) {
        return false
    }
    // 是否写新库
    isPort(flag) {
        return true
    }

    // 读key
    rdKey() { return 'switch:' + this.module + ':rd:' + this.table }
    // 写key
    rwKey() { return 'switch:' + this.module + ':rw:' + this.table }

    // debug
    async debug() {
        const flagWrite = await this.flagForWrite()
        const flagRead = await this.flagForRead()

        const obj = {
            'key_read': this.rdKey(),
            'flag_read': this.isPort(flagRead) ? 'atlas' : 'gcp',
            'key_write': this.rwKey(),
            'flag_write': this.isDual(flagWrite) ? 'dual' : this.isPort(flagWrite) ? 'gcp' : 'atlas',
            'table': this.table,
            'module': this.module,
        }

        console.log(obj)
        return obj
    }
}

module.exports = { ModelEngine }
