from fastapi import APIRouter, Depends
from enum import Enum
from pydantic import BaseModel
from src.api import auth

import sqlalchemy
from src import database as db
from sqlalchemy.exc import IntegrityError


router = APIRouter(
    prefix="/bottler",
    tags=["bottler"],
    dependencies=[Depends(auth.get_api_key)],
)

class PotionInventory(BaseModel):
    potion_type: list[int]
    quantity: int

@router.post("/deliver/{order_id}")
def post_deliver_bottles(potions_delivered: list[PotionInventory], order_id: int):
    """ 
    """
    ml_to_subtract= {
                        'red': 0,
                        'green': 0,
                        'blue': 0,
                        'dark': 0
                    }
    
    potion_list_of_dicts = []
    for potion in potions_delivered:
        potion_list_of_dicts.append({
                                        "potion_type": potion.potion_type, 
                                        "transaction": potion.quantity
                                        })
                                    

    
    for potion in potions_delivered:
        for mix_color, sub_color in zip(potion.potion_type, ml_to_subtract.keys()):
            ml_to_subtract[sub_color] -= (mix_color * potion.quantity)

    update_ml_sql = sqlalchemy.text("""
                    INSERT INTO ml_ledger (red, green, blue, dark, game_day, game_hr)
                    VALUES (:red, :green, :blue, :dark, (SELECT day FROM curr_time), (SELECT hour FROM curr_time))
                    """)

    
    update_potion_ledger_sql = sqlalchemy.text("""
                    WITH subquery AS (
                        SELECT id AS potion_id
                        FROM potion_inventory
                        WHERE potion_type = :potion_type
                    )
                    INSERT INTO potion_ledger (transaction, potion_id, reason)
                    SELECT :transaction, subquery.potion_id, 'Bobo be bottling'
                    FROM subquery
                                                """)

    try:
        with db.engine.begin() as connection:
            connection.execute(update_ml_sql, ml_to_subtract)
            connection.execute(update_potion_ledger_sql, potion_list_of_dicts)

            try:
                connection.execute(sqlalchemy.text("""INSERT INTO processed (job_id, type)
                                                            VALUES (:order_id, 'bottler')
                                                        """), {"order_id": order_id})
            except IntegrityError as e:
                print(f"Tried to deliver bottles again on job_id: {order_id}")
                return "ALREADY_PROCESSED"

    except Exception as e:
        print(f"Error trying to deliver bottled potions: {e}")
        return "SAD"
    
    return "OK"

@router.post("/plan")
def get_bottle_plan():
    """
    Go from barrel to bottle.
    """

    # Each bottle has a quantity of what proportion of red, blue, and
    # green potion to add.
    # Expressed in integers from 1 to 100 that must sum up to 100.
    
    # "potion_type": [r, g, b, d]
    # Each potion is 100ml

    bottle_plan = []

    avail_color = {"red": 0, "green": 0, "blue": 0, "dark": 0}

    ml_sql = sqlalchemy.text("""
                            SELECT
                                SUM(red) AS total_red,
                                SUM(green) AS total_green,
                                SUM(blue) AS total_blue,
                                SUM(dark) AS total_dark
                            FROM ml_ledger;
                             """)

    potion_sql = sqlalchemy.text("""
                                    WITH subquery AS (
                                            SELECT potion_id, SUM(transaction) AS potion_quant
                                            FROM potion_ledger
                                            GROUP BY potion_id
                                        )
                                    SELECT id, name, subquery.potion_quant as quantity, price, potion_type, bottle_goal
                                    FROM potion_inventory
                                    JOIN subquery ON subquery.potion_id = potion_inventory.id
                                    ORDER BY quantity ASC
                                    """)
    
    try:
        with db.engine.begin() as connection:
            avail_ml = connection.execute(ml_sql).fetchone()
            potion_inventory = connection.execute(potion_sql)
    except Exception as e:
        print(f"Error grabbing potion inventories: {e}")
        return []

    potion_columns = potion_inventory.keys()
    mix_dict = [dict(zip(potion_columns, row)) for row in potion_inventory.fetchall()]

    total_avail_ml = 0
    for key, value in zip(avail_color.keys(), avail_ml):
        avail_color[key] = value
        total_avail_ml += value


                
    if total_avail_ml < 100:
        print(f"Not enough ml --> no potions bottled")
        return []
    else:

        #logic for what to bottle
        #only adding potions that have less than 10 in inventory
        #later optimize based on game days or whatever
        for mix in mix_dict:
            can_bottle = False
            update_quant = mix['bottle_goal'] - mix['quantity'] 
            if update_quant >= mix['bottle_goal']:
                update_quant = mix['bottle_goal']
            if update_quant > 0:
                for mix_color, have_color in zip(mix['potion_type'], avail_color.keys()):

                    if avail_color[have_color] < (mix_color * update_quant):
                        while (avail_color[have_color] < (mix_color * update_quant)) and (update_quant > 0):
                            update_quant -= 1
                    if avail_color[have_color] >= (mix_color * update_quant):
                        #reaching end of potion type w/o failure
                        if have_color == 'dark' and avail_color[have_color] >= mix_color:
                            can_bottle = True
                    
                if can_bottle and update_quant != 0:
                    bottle_plan.append({'potion_type': mix['potion_type'], 'quantity': update_quant})
                    for mix_color, have_color in zip(mix['potion_type'], avail_color.keys()):
                        avail_color[have_color] -= (mix_color * update_quant)



        print(f"Pre-bottle potion inventory is: {mix_dict}\n")
        print(f"Bottle plan is {bottle_plan}")
        return bottle_plan
    


if __name__ == "__main__":
    print(get_bottle_plan())