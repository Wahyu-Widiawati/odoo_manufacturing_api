import psycopg2
import xmlrpc.client

#Postgre Configuration
pg_host = ' '
pg_port = ' '
pg_user = ' '
pg_password = ' '
pg_dbname = ' '

#connect to postgresql
conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname
)

#Odoo Confifguration
odoo_url = 'http://'
odoo_db = ' '
odoo_username = ' '
odoo_password = ' '

#Establish a connection to the Odoo server
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
uid = common.authenticate(odoo_db, odoo_username, odoo_password, {})

# Connect to the Odoo object
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))

# Fetch data from PostgreSQL
def get_production_plan_data():
    query = "select date_start, company_id, priority, product_id, uom_id, qty_producing, product_qty, picking_type_id, bom_id from production_plan_full where bom_id is not null"
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows
    
def get_company_id(name):
    company = models.execute_kw(odoo_db, uid, odoo_password,
                                'res.company', 'search_read',
                                [[['name', '=', name]]],
                                {'fields': ['id'], 'limit': 1})
    return company[0]['id'] if company else None

# Fetch data from PostgreSQl
production_plan_data = get_production_plan_data()

# Loop each records to create manufacturing orders in Odoo
for order in production_plan_data:
    date_start, company_name, priority, product_id, uom_id, qty_producing, product_qty, picking_type_id, bom_id = order

    company_id = get_company_id(company_name)

    if company_id is None:
        print(f"Company '{company_name}' not found.")
        continue

    # Convert date_start to string
    date_start = date_start.strftime('%Y-%m-%d')

    # Convert decimal.Decimal to float or string
    qty_producing = float(qty_producing)
    product_qty = float(product_qty)

    order_data = {
        'date_start': date_start,
        'company_id': 1,
        'priority': 0,
        'product_id': product_id,
        'product_uom_id': uom_id,
        'qty_producing': qty_producing,
        'product_qty': product_qty,
        'picking_type_id': picking_type_id,
        'bom_id': bom_id,
    }
    try:
        manufacturing_order_id = models.execute_kw(odoo_db, uid, odoo_password,
                                                   'mrp.production', 'create',
                                                   [order_data])
        print(f"Created Manufacturing Order ID: {manufacturing_order_id}")
    except Exception as e:
        print(f"Error creating Manufacturing Order for product_id{product_id}:{e}")

# Close the PostgreSQL connection
conn.close()
