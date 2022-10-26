import os
from flask import Flask
from my_functions.functions import MakeQuery

from my_functions.functions import *
from my_functions.etl import *

query = '''select avg(p.precio) from sucursal as s
join precios as p on (s.id = p.sucursal_id)
where s.id = '91688';'''

resultado = MakeQuery()

app = Flask(__name__)

@app.route("/")
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Buenas tardes, el resultado de su QUERY es {}!".format(resultado)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8081)))

