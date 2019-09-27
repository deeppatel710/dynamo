import flask
from flask import request, jsonify, abort, make_response, render_template
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import json


SF_USER='dpatel'
SF_PASSWORD='rstockDP710!'
SF_ACCOUNT='qc83684'
SF_DATABASE='DATASCIENCE_DB'
SF_SCHEMA='TEMP'
SF_WAREHOUSE='fivetran_wh'

snowflake_connection_string = "snowflake://{}:{}@{}/{}/{}?warehouse={}".format(SF_USER,SF_PASSWORD,SF_ACCOUNT,SF_DATABASE,SF_SCHEMA,SF_WAREHOUSE)


app = flask.Flask(__name__, template_folder='template')
app.config["DEBUG"] = True


@app.route('/get-markets/', methods=['POST'] )
def get_markets():
	json_req = request.get_json()

	market_types = json_req['market_type']

	tech_hub = "(True)" if 0 in market_types else '(true, False)'
	young_professionals = True
	near_forbes_500_company = True
	investor_favorite = "(True)" if 3 in market_types else '(true, False)'
	hot_on_roofstock = "(True)" if 4 in market_types else '(true, False)'
	undervalued_markets = True

	starter_home_price = None 

	if json_req['price_bucket'] == [0,1]:
		starter_home_price = '(0,1)'
	elif json_req['price_bucket'] == [0]:
		starter_home_price = '(0)'
	elif json_req['price_bucket'] == [1]:
		starter_home_price = '(1)'

	if 'price_volatility' in json_req:
		price_volatility = "(" + json_req['price_volatility'] + ")"
	else:
		price_volatility = "(true,False)"

	if 'competitiveness' in json_req:
		competitiveness = "(" + json_req['competitiveness'] + ")"
	else:
		competitiveness = "(True,False)"



	engine = create_engine(snowflake_connection_string)
	connection = engine.connect()


	query_all_data = '''select * from 
		datascience_db.temp.hackathon_data a
		LEFT JOIN(
		  select mc.cbsa_code, m.market_id, m.market_name
			from "DATAMARTS_DB"."ROOFSTOCK_DM"."DIM_MARKET" m 
			left join "DATAMARTS_DB"."ROOFSTOCK_DM"."DIM_MARKETCBSA" mc on m.market_id = mc.market_id
			group by 1,2, 3
			order by 1,2, 3
		  ) b
		on a.cbsacode = b.cbsa_code
		where tech_hub in {}
		 and young_professionals = {}
		 and near_forbes_500_company = {}
		 and investor_favorite in {}
		 and hot_on_roofstock in {}
		 and undervalued_markets = {}
		 and starter_home_price in {}
		 and price_volatility in {}
		 and competitiveness in {}'''.format(tech_hub, young_professionals, near_forbes_500_company, investor_favorite,  hot_on_roofstock,undervalued_markets, starter_home_price, price_volatility, competitiveness )

	all_data = {}
	query_pop_growth = '''select * from datascience_db.temp.households'''
	try:
		df1 = pd.read_sql(query_all_data, connection, index_col=None)
		for index, row in df1.iterrows():
			obj =  {		"market_id": row["market_id"] if row["market_id"] else 'null',
							"display_name": row['market_name'].upper() if row['market_name'] else row['msaname'].upper(),
							"expected_cash_flow": row['yield'],
							"HPA": row['market_growth'],
							"population_growth": row['population_growth'],
							"job_growth": row['job_growth'],
							"income_growth": row['income_growth']
					}
			
			all_data[int(row['cbsacode'])] = obj


	except Exception as e:
		raise Exception(e)

	return all_data, 200, {'Content-Type': 'application/json; charset=utf-8'}


@app.route('/get-all/', methods=['GET'] )
def get_all_markets():

	engine = create_engine(snowflake_connection_string)
	connection = engine.connect()
	
	query_all_data = '''select * from 
		datascience_db.temp.hackathon_data a
		LEFT JOIN(
		  select mc.cbsa_code, m.market_id, m.market_name
			from "DATAMARTS_DB"."ROOFSTOCK_DM"."DIM_MARKET" m 
			left join "DATAMARTS_DB"."ROOFSTOCK_DM"."DIM_MARKETCBSA" mc on m.market_id = mc.market_id
			group by 1,2, 3
			order by 1,2, 3
		  ) b
		on a.cbsacode = b.cbsa_code'''
		
	all_data = {}
	try:
		df1 = pd.read_sql(query_all_data, connection, index_col=None)
		for index, row in df1.iterrows():
			obj =  {		"market_id": row["market_id"] if row["market_id"] else 'null',
							"display_name": row['market_name'].upper() if row['market_name'] else row['msaname'].upper(),
							"expected_cash_flow": row['yield'],
							"HPA": row['market_growth'],
							"population_growth": row['population_growth'],
							"job_growth": row['job_growth'],
							"income_growth": row['income_growth']
					}
			
			all_data[int(row['cbsacode'])] = obj


	except Exception as e:
		raise Exception(e)

	return all_data, 200, {'Content-Type': 'application/json; charset=utf-8'}








