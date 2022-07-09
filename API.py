from flask import Flask,jsonify
from Spark_Dataframe_Query import Query
app = Flask(__name__)


@app.route('/')
def home():
    return "Start Mock_Project"

@app.route("/api/task_1")
def task_1():
    Obj = Query()
    return jsonify(Obj.task_1())

@app.route("/api/task_2")
def task_2():
    Obj = Query()
    return jsonify(Obj.task_2())

@app.route("/api/task_3")
def task_3():
    Obj = Query()
    return jsonify(Obj.task_3())

@app.route("/api/task_4")
def task_4():
    Obj = Query()
    return jsonify(Obj.task_4())

@app.route("/api/task_5")
def task_5():
    Obj = Query()
    return jsonify(Obj.task_5())

@app.route("/api/task_6")
def task_6():
    Obj = Query()
    return jsonify(Obj.task_6())

@app.route("/api/task_7")
def task_7():
    Obj = Query()
    return jsonify(Obj.task_7())

@app.route("/api/task_8")
def task_8():
    Obj = Query()
    return jsonify(Obj.task_8())

@app.route("/api/task_9")
def task_9():
    Obj = Query()
    return jsonify(Obj.task_9())

if __name__ == "__main__":
    app.run(debug=False)