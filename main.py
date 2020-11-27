from flask import Flask, render_template, request
from parser1 import site_parsing
from parser2 import site_parsing_2

app = Flask(__name__)

@app.route('/index')
def hello():
    return render_template('index.html')

@app.route('/contacts')
def contact():
    return render_template('contacts.html')

@app.route('/parsing')
def parsing():
    return render_template('parsing.html')

@app.route('/pars_res')
def parser():
    car_name_site, site_name, average_price, max_price, min_price, offers_all = site_parsing()
    return render_template('pars_res.html', car_name_site = car_name_site, site_name = site_name, average_price = average_price, max_price = max_price, min_price = min_price, offers_all = offers_all)

@app.route('/pars_res2')
def parser_2():
    data = site_parsing_2()
    return render_template('pars_res2.html', **data)


@app.route('/form', methods = ['POST'])
def moto_form():
    brand = request.form['brand']
    return render_template('form.html', brand = brand)



if __name__ == '__main__':
    app.run(debug = True)