from flask import Flask, jsonify, request, abort
from bjdb import BJDB
from utils import log


app = Flask(__name__)
db = BJDB('bj.db')


@app.route('/', methods=['GET'])
def tables():
    return jsonify(db.tables())


@app.route('/tables/<table>/headers', methods=['GET'])
def headers(table):
    return jsonify(db.headers(table))


@app.route('/new', methods=['POST'])
def new_tables():
    data = request.get_json()
    if data.get('table') and data.get('headers'):
        db.create_table(data['table'], data['headers'])
        return '', 204

    else:
        return abort(400)


@app.route('/tables/<table>', methods=['POST'])
def insert(table):
    data = request.get_json()
    try:
        db.insert(table, data)
        return '', 204
    except Exception as e:
        log(e)
        return abort(400)


def make_cond(key_data):
    def cond(record):
        for k, v in key_data.items():
            if (not record.get(k)) or (record[k] != v):
                return False
        else:
            return True

    return cond


@app.route('/tables/<table>/search', methods=['GET'])
def search(table):
    key_data = request.args
    return jsonify(db.search(table, make_cond(key_data)))


@app.route('/tables/<table>', methods=['PUT'])
def update(table):
    data = request.get_json()
    try:
        new_data = data['newdata']
        key_data = data['keydata']
        db.update(table, new_data, make_cond(key_data))
        return '', 204

    except Exception as e:
        log(e)
        return abort(400)


@app.route('/tables/<table>', methods=['DELETE'])
def delete(table):
    data = request.get_json()
    try:
        db.delete(table, make_cond(data))
        return '', 204

    except Exception as e:
        log(e)
        return abort(400)


@app.route('/tables/<table>/all', methods=['GET'])
def all_data(table):
    return jsonify(db.all(table))


@app.route('/merge', methods=['POST'])
def merge():
    db.merge()
    return '', 204


@app.route('/tables/<table>/all', methods=['DELETE'])
def purge(table):
    db.purge(table)
    return '', 204


if __name__ == '__main__':
    app.run()
