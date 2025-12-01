from fastapi import APIRouter, Depends
from pydantic import BaseModel
from src.api import auth

import sqlalchemy
from src import database as db

import random

"""
WHOLESALE CATALOG
[Barrel(sku='SMALL_RED_BARREL', ml_per_barrel=500, potion_type=[1, 0, 0, 0], price=100, quantity=10),
Barrel(sku='SMALL_GREEN_BARREL', ml_per_barrel=500, potion_type=[0, 1, 0, 0], price=100, quantity=10),
Barrel(sku='SMALL_BLUE_BARREL', ml_per_barrel=500, potion_type=[0, 0, 1, 0], price=120, quantity=10),

Barrel(sku='MEDIUM_RED_BARREL', ml_per_barrel=2500, potion_type=[1, 0, 0, 0], price=250, quantity=10),  
Barrel(sku='MEDIUM_GREEN_BARREL', ml_per_barrel=2500, potion_type=[0, 1, 0, 0], price=250, quantity=10), 
Barrel(sku='MEDIUM_BLUE_BARREL', ml_per_barrel=2500, potion_type=[0, 0, 1, 0], price=300, quantity=10), 
 
Barrel(sku='MINI_RED_BARREL', ml_per_barrel=200, potion_type=[1, 0, 0, 0], price=60, quantity=1), 
Barrel(sku='MINI_GREEN_BARREL', ml_per_barrel=200, potion_type=[0, 1, 0, 0], price=60, quantity=1), 
Barrel(sku='MINI_BLUE_BARREL', ml_per_barrel=200, potion_type=[0, 0, 1, 0], price=60, quantity=1), 

Barrel(sku='LARGE_DARK_BARREL', ml_per_barrel=10000, potion_type=[0, 0, 0, 1], price=750, quantity=10), 
Barrel(sku='LARGE_BLUE_BARREL', ml_per_barrel=10000, potion_type=[0, 0, 1, 0], price=600, quantity=30), 
Barrel(sku='LARGE_GREEN_BARREL', ml_per_barrel=10000, potion_type=[0, 1, 0, 0], price=400, quantity=30), 
Barrel(sku='LARGE_RED_BARREL', ml_per_barrel=10000, potion_type=[1, 0, 0, 0], price=500, quantity=30)]
"""

router = APIRouter(
    prefix="/barrels",
    tags=["barrels"],
    dependencies=[Depends(auth.get_api_key)],
)

class Barrel(BaseModel):
    sku: str
    ml_per_barrel: int
    potion_type: list[int]
    price: int
    quantity: int


@router.post("/deliver/{order_id}")
def post_deliver_barrels(barrels_delivered: list[Barrel], order_id: int):
    """ 
    This code should actually change the database
    """

    quantity_plan = {
                        "red": 0,
                        "green": 0,
                        "blue": 0, 
                        "dark": 0
                    }
    gold_to_pay = 0

    for barrel in barrels_delivered:
        type = barrel.potion_type
        gold_to_pay -= (barrel.quantity * barrel.price)
        delivered_ml = (barrel.quantity * barrel.ml_per_barrel)
        match type:
            case [1,0,0,0]:
                quantity_plan['red'] = quantity_plan['red'] + delivered_ml
            case [0,1,0,0]:
                quantity_plan['green'] = quantity_plan['green'] + delivered_ml
            case [0,0,1,0]:
                quantity_plan['blue'] = quantity_plan['blue'] + delivered_ml
            case [0,0,0,1]:
                quantity_plan['dark'] = quantity_plan['dark'] + delivered_ml
        

    barrel_ml_sql = sqlalchemy.text("""
                                    INSERT INTO ml_ledger (red, green, blue, dark, game_day, game_hr)
                                    VALUES (:red, :green, :blue, :dark, 
                                            (SELECT day FROM curr_time), 
                                            (SELECT hour FROM curr_time)
                                            )
                                    """)
    
    payment_sql = sqlalchemy.text("""
                                  INSERT INTO gold_ledger (transactions, game_day, game_hr, reason)
                                  SELECT :transaction, 
                                        (SELECT day FROM curr_time), 
                                        (SELECT hour FROM curr_time), 
                                        :reason
                                  """)
    
    try:
        print(f"Attempt to deliver ml amt: {quantity_plan}")
        print(f"This barrel delivery would cost: {gold_to_pay}")
        with db.engine.begin() as connection:
                connection.execute(barrel_ml_sql, quantity_plan)
                connection.execute(payment_sql, {"transaction": gold_to_pay, "reason": 'barrel delivery'})
    except Exception as e:
        print(f"Error delivering barrels: {e}")
    
    return "OK"

# Gets called once a day
@router.post("/plan")
def get_wholesale_purchase_plan(wholesale_catalog: list[Barrel]):
    """ 
    """
    print(wholesale_catalog)

    sm_buying_plan_dict = []
    med_buying_plan_dict = []
    lg_buying_plan_dict = []

    buying_plan_dict = []

    for barrel in wholesale_catalog:
        barrel_size = barrel.ml_per_barrel
        barrel_type = barrel.potion_type
        match barrel_size:
            case 500: #small barrels
                barrel = barrel.__dict__
                barrel['in_catalog'] = barrel['quantity']
                barrel['quantity'] = 0
                sm_buying_plan_dict.append(barrel)
            case 2500: #medium barrels
                barrel = barrel.__dict__
                barrel['in_catalog'] = barrel['quantity']
                barrel['quantity'] = 0
                med_buying_plan_dict.append(barrel)
            case 10000: #large barrels
                barrel = barrel.__dict__
                barrel['in_catalog'] = barrel['quantity']
                barrel['quantity'] = 0
                lg_buying_plan_dict.append(barrel)
                if barrel['potion_type'] == [0,0,0,1]:
                    med_buying_plan_dict.append(barrel)

    gold_qry = "SELECT SUM(transactions) FROM gold_ledger"
    ml_qry = "SELECT SUM(red) as red, SUM(green) as green, SUM(blue) as blue, SUM(dark) as dark FROM ml_ledger"    
    capacity_qry = "SELECT sum(ml) FROM capacity"
    goal_ml_qry = "SELECT med_goal, lg_goal, low_ml_limit FROM goal_ml"

    try:
        with db.engine.begin() as connection:
            avail_gold = connection.execute(sqlalchemy.text(gold_qry)).scalar()
            inventory_ml = connection.execute(sqlalchemy.text(ml_qry)).fetchone()
            curr_capacity = connection.execute(sqlalchemy.text(capacity_qry)).scalar()
            goal_ml = connection.execute(sqlalchemy.text(goal_ml_qry)).fetchone()

    except Exception as e:
        print(f"Error in transaction for barrel plan: {e}")
        return buying_plan_dict #empty list

    # current capacity level minus ml already have
    curr_ml = sum(inventory_ml)
    avail_ml = curr_capacity - curr_ml

    print(f"Current capacity for ml is: {curr_capacity}")
    print(f"Ml in inventory: {curr_ml}")
    print(f"Avail. for ml: {avail_ml}\n")

    desp_level = goal_ml.low_ml_limit # set by table in db
    med_planned = goal_ml.med_goal
    lg_planned = goal_ml.lg_goal if goal_ml.lg_goal <= avail_ml else avail_ml

    low_ml = False
    if inventory_ml.red <= desp_level or inventory_ml.green <= desp_level or inventory_ml.blue <= desp_level:
        low_ml = True

    if avail_gold >= 2500 and lg_buying_plan_dict \
        and curr_capacity >= 40000:

        buying_plan_dict = lg_buying_plan_dict
        print(f"Room we actually have: {avail_ml} ml")
        print(f"Goal ml for large that we're doing: {goal_ml.lg_goal} ml\n")
        avail_ml = lg_planned

    elif avail_gold >= 800 and med_buying_plan_dict \
        and avail_ml >= 5000 and low_ml: #only get med if desperately low & and no large barrel

        buying_plan_dict = med_buying_plan_dict
        print(f"We're desperate, going with db medium barrel goal plan: {goal_ml.med_goal} ml")
        avail_ml = med_planned

    elif avail_gold >= 100 \
        and curr_capacity <= 10000: #only get small in beginning
        buying_plan_dict = sm_buying_plan_dict


    #sort list so that least ml prioritized
    for barrel in buying_plan_dict:
        potion_type = barrel['potion_type']
        barrel['reached_max'] = False
        match potion_type:
            case [1,0,0,0]:
                barrel['curr_ml'] = inventory_ml.red
            case [0,1,0,0]:
                barrel['curr_ml'] = inventory_ml.green
            case [0,0,1,0]:
                barrel['curr_ml'] = inventory_ml.blue
            case [0,0,0,1]:
                barrel['curr_ml'] = inventory_ml.dark

    buying_plan_dict = sorted(buying_plan_dict, key=lambda k: k['curr_ml'])
   
    gold_to_pay = 0
    ml_to_add = 0
    at_max = False
    reached_max = 0
    while not at_max and buying_plan_dict:
        for barrel in buying_plan_dict:
            gold_check = gold_to_pay + (barrel['price'])
            ml_check =  ml_to_add + (barrel['ml_per_barrel'])
            if reached_max >= len(buying_plan_dict):
                at_max = True
                print(f"Reached max quantity of barrels from catalog")
                break
            elif avail_gold >= gold_check and avail_ml >= ml_check:
                if barrel['quantity'] < barrel['in_catalog']:
                    barrel['quantity'] += 1
                    gold_to_pay += barrel['price']
                    ml_to_add += barrel['ml_per_barrel']
                elif not barrel['reached_max']:
                        barrel['reached_max'] = True
                        reached_max += 1
            else:
                if avail_gold < gold_check:
                    print(f"Reached max gold for barrel plan")
                if avail_ml < ml_check:
                    print(f"Reached max ml for barrel plan")
                at_max = True
                break

    # remove keys used for sorting and stuff
    for barrel in buying_plan_dict:
        barrel.pop('curr_ml', None)
        barrel.pop('in_catalog', None)
        barrel.pop('reached_max', None)

    buying_plan_dict = [barrel for barrel in buying_plan_dict if barrel['quantity'] != 0]

    print(f"Gold that this plan will cost: {gold_to_pay}")
    print(f"Gold that I have: {avail_gold}")
    print(f"Total ml that this plan will add: {ml_to_add}")
    print(f"Total ml that there is room for: {avail_ml}\n")

    print(f"Barrel buying plan: {buying_plan_dict}")

    return buying_plan_dict

       