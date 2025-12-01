from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from src.api import auth
from enum import Enum

import sqlalchemy
from src import database as db

from fastapi import HTTPException, status

from sqlalchemy.exc import IntegrityError

router = APIRouter(
    prefix="/carts",
    tags=["cart"],
    dependencies=[Depends(auth.get_api_key)],
)

class search_sort_options(str, Enum):
    customer_name = "customer_name"
    item_sku = "item_sku"
    line_item_total = "line_item_total"
    timestamp = "timestamp"

class search_sort_order(str, Enum):
    asc = "asc"
    desc = "desc"   

@router.get("/search/", tags=["search"])
def search_orders(
    customer_name: str = "",
    potion_sku: str = "",
    search_page: str = "",
    sort_col: search_sort_options = search_sort_options.timestamp,
    sort_order: search_sort_order = search_sort_order.desc,
):
    """
    Search for cart line items by customer name and/or potion sku.

    Customer name and potion sku filter to orders that contain the 
    string (case insensitive). If the filters aren't provided, no
    filtering occurs on the respective search term.

    Search page is a cursor for pagination. The response to this
    search endpoint will return previous or next if there is a
    previous or next page of results available. The token passed
    in that search response can be passed in the next search request
    as search page to get that page of results.

    Sort col is which column to sort by and sort order is the direction
    of the search. They default to searching by timestamp of the order
    in descending order.

    The response itself contains a previous and next page token (if
    such pages exist) and the results as an array of line items. Each
    line item contains the line item id (must be unique), item sku, 
    customer name, line item total (in gold), and timestamp of the order.
    Your results must be paginated, the max results you can return at any
    time is 5 total line items.
    """
    search_param = {
        "customer_name": customer_name,
        "potion_sku": potion_sku,
        "search_page": search_page,
        "sort_col": sort_col.value,
        "sort_order": sort_order.value
    }

    print(f"Search parameters are: {search_param}")

    customer_name = "%" + customer_name + "%"
    potion_sku = "%" + potion_sku + "%"

    sort_col_val = sort_col.value

    match sort_col_val:
        case 'customer_name':
            sort_col_val = "customers.cust_name"
        case 'item_sku':
            sort_col_val = "line_items.item_sku"
        case 'line_item_total':
            sort_col_val = "(line_items.quantity*line_items.price)"
        case 'timestamp':
            sort_col_val = "line_items.created_at"

    if not search_page:
        search_page = 0
    else: 
        search_page = int(search_page)

    if search_page >= 1:
        search_offset = 5*(search_page)
        prev_pg = str(search_page - 1)
        if not prev_pg:
            prev_pg = ""
    elif not search_page:
        search_offset = 0
        prev_pg = ""

    next_pg = str(search_page + 1)


    search_dict = {"name_search": customer_name, 
                   "sku_search": potion_sku,
                   "page_num": search_offset
                   }
    

    results_sql = sqlalchemy.text(f"""SELECT line_items.line_id as line_item_id,
                                        line_items.item_sku,
                                        customers.cust_name as customer_name,
                                        (line_items.quantity*line_items.price) as line_item_total,
                                        line_items.created_at as timestamp
                                    FROM line_items
                                    JOIN carts ON line_items.cart_id = carts.id
                                    JOIN customers ON carts.cust_id = customers.id
                                    WHERE cust_name ILIKE :name_search
                                        AND item_sku ILIKE :sku_search
                                    ORDER BY {sort_col_val} {sort_order.value} """)
    
    results_list = []
    
    try:
        with db.engine.begin() as connection:
            results = connection.execute(results_sql, search_dict)

        results_columns = results.keys()
        results = results.fetchall()
        results_length = len(results)

        all_results_list = [dict(zip(results_columns, row)) for row in results]

        start_index = 5*search_page
        finish_index = start_index + 5

        results_list = all_results_list[start_index:finish_index]
        
        if finish_index >= results_length:
            next_pg = "" 
        print(f"Number of search results: {results_length}")

    except Exception as e:
        print(f"Error trying to grab line item results: {e}")

    return {
        "previous": prev_pg,
        "next": next_pg,
        "results": results_list
    }


class Customer(BaseModel):
    customer_name: str
    character_class: str
    level: int

""""
print(customers) print example list

[Customer(customer_name='Becmmok', character_class='Druid', level=1), 
Customer(customer_name='Theodora Oakenshield', character_class='Druid', level=2)]
"""

@router.post("/visits/{visit_id}")
def post_visits(visit_id: int, customers: list[Customer]):
    """
    Which customers visited the shop today?
    """
    print(customers)
    print(f"Customer count this tick: {len(customers)}")

    customer_list = []
    #get passed in empty customer list >.>
    if not customers:
        print('Uuuuuuuuh, no customers came :(')
    else:
        for customer in customers:
            customer_list.append({
                "cust_name": customer.customer_name,
                "cust_class": customer.character_class,
                "level": customer.level            })

        customer_visit_sql = sqlalchemy.text("""INSERT INTO customers (cust_name, cust_class, level)
                                                SELECT :cust_name, :cust_class, :level
                                                WHERE NOT EXISTS (
                                                    SELECT 1 FROM customers 
                                                    WHERE cust_name = :cust_name
                                                    AND cust_class = :cust_class
                                                    AND level = :level
                                                )
                                                LIMIT 1
                                            """)
        
        with db.engine.begin() as connection:
            connection.execute(customer_visit_sql, customer_list)
    
    return "OK"


time_qry = []
@router.post("/")
def create_cart(new_cart: Customer):
    """ 
    LOGIC: insert into carts db for each new created cart, will link back to a single customer id
    -one unique customer id can have multiple cart ids
    """

    create_time = {}
    day_sql = """SELECT day, hour
                 FROM curr_time
                """

    curr_customer = {
                        "cust_name": new_cart.customer_name,
                        "cust_class": new_cart.character_class,
                        "level": new_cart.level,
                    }
    
    search_cust = sqlalchemy.text("""SELECT id
                     FROM customers
                     WHERE (cust_name = :cust_name
                            AND cust_class = :cust_class
                            AND level = :level)
                    """)

    insert_cart_sql = sqlalchemy.text("""
                                    INSERT INTO carts (cust_id, game_day, game_hr)
                                    VALUES (:cust_id, :game_day, :game_hr)
                                    RETURNING carts.id
                                    """)
    
        
    with db.engine.begin() as connection:
        time_qry = connection.execute(sqlalchemy.text(day_sql)).fetchone()
        #print(f"Time query: {time_qry}")
        customer = connection.execute(search_cust, curr_customer).scalar()
        create_time = {
                    "cust_id": customer,
                    "game_day": time_qry.day,
                    "game_hr": time_qry.hour
                  }
        unique_cart = connection.execute(insert_cart_sql, create_time).scalar()

    #test logs
    print(f"Unique cart #{unique_cart} for customer {curr_customer['cust_name']}")

    return {"cart_id": unique_cart}


class CartItem(BaseModel):
    quantity: int


@router.post("/{cart_id}/items/{item_sku}")
def set_item_quantity(cart_id: int, item_sku: str, cart_item: CartItem):
    """ 
    LOGIC: update the quantity of a specific item in a cart

    REQUEST: 
    {
        "quanitity": "integer"
    }

    RESPONSE:
    {
        "success": boolean
    }
    """

    line_dict = {
                    "cart_id": cart_id,
                    "item_sku": item_sku,
                    "quantity": cart_item.quantity
                }

    
    lineitem_sql = sqlalchemy.text("""
                                    INSERT INTO line_items (cart_id, item_sku, potion_id, quantity, price)
                                    VALUES (:cart_id, :item_sku, 
                                            (SELECT id FROM potion_inventory WHERE sku = :item_sku), 
                                            :quantity, 
                                            (SELECT price FROM potion_inventory WHERE sku = :item_sku))
                                    ON CONFLICT DO NOTHING
                                    RETURNING line_items.line_id
                                   """)
    
    try:
        with db.engine.begin() as connection:
            connection.execute(lineitem_sql, line_dict)
    except Exception as e:
        print(f"Error with setting item quantity: {e}")

    return "OK"




class CartCheckout(BaseModel):
    payment: str

@router.post("/{cart_id}/checkout")
def checkout(cart_id: int, cart_checkout: CartCheckout):
    """ 
    LOGIC: CartCheckout has property: payment
    """

    checkout_summary = {
        'total_potions_bought':0, 
        'total_gold_paid': 0
        }
    already_processed = False
    
    deposit_sql = sqlalchemy.text("""INSERT INTO gold_ledger (transactions, game_day, game_hr, reason, cart_id)
                                 VALUES ( (SELECT sum(quantity * price) FROM line_items WHERE cart_id = :cart_id), 
                                          (SELECT day FROM curr_time), 
                                          (SELECT hour FROM curr_time), 
                                            'potion checkout', 
                                            :cart_id)
                                  RETURNING transactions
                                """)
    
    potion_subtract_sql = sqlalchemy.text("""
                                          WITH line_item_lookup AS 
                                                (SELECT (-1*quantity) AS potion_amt, item_sku, potion_id
                                                FROM line_items
                                                WHERE cart_id = :cart_id)
                                          INSERT INTO potion_ledger (potion_id, transaction, cart_id, reason)
                                          SELECT potion_id, potion_amt, :cart_id, 'cart checkout'
                                          FROM line_item_lookup
                                          RETURNING transaction""")
    
    with db.engine.begin() as connection:
        
        paid_gold = connection.execute(deposit_sql, {"cart_id": cart_id}).scalar()
        potion_amt_sold = connection.execute(potion_subtract_sql, {"cart_id": cart_id}).scalar()

        try:
            connection.execute(sqlalchemy.text("""INSERT INTO processed (job_id, type)
                                                        VALUES (:cart_id, 'checkout')
                                                    """), {"cart_id": cart_id})
        except IntegrityError as e:
            already_processed = True
            print(f"Tried to call cart_checkout again on cart_id: {cart_id}")


    checkout_summary['total_potions_bought'] = -1 * potion_amt_sold
    checkout_summary['total_gold_paid'] = paid_gold
    
    print(f"Customer with cart id: {cart_id} paid with {cart_checkout}\nSummary of checkout: {checkout_summary}")
        
    return checkout_summary
