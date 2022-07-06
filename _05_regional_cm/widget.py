from utils.config import PathReader
import os
import tkinter as tk
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

def widget_start():

    fname = "(REGIONAL_CM)raw_{}.xlsx".format(
        datetime.now().strftime("%y") + (datetime.now() - relativedelta(months=1)).strftime("%m"))

    def input_data():
        global method
        method = "input_data"
        root.quit()

    def excel_data():
        global method
        method = "excel_data"
        root.quit()

    root = tk.Tk()
    root.title("유지보수 : by jaesang.lee_b@kurlycorp.com")
    root.geometry("640x150+100+100")
    root.resizable(False, False)

    tk.Label(root, text = "Input 데이터를 직접입력하시겠습니까?(Input)", fg="black").grid(row = 0, sticky = "W")
    tk.Label(root, text = "엑셀로 로드하겠습니까?(Excel)", fg="black").grid(row = 1, sticky = "W")
    tk.Label(root, text = f"(엑셀명:{fname}로 설정해주세요)", fg="black").grid(row = 2, sticky = "W")

    tk.Button(root, text="직접입력(Input)", command=input_data, fg="gray").grid(row=0, column=1)
    tk.Button(root, text="엑셀입력(Excel)", command=excel_data, fg="gray").grid(row=2, column=1)
    tk.mainloop()

def widget_input_data():

    def get_input():
        gmv_input = int(gmv.get())
        orders_input = int(orders.get())
        purchasers_input = int(purchasers.get())
        cogs_ratio_input = float(cogs_ratio.get())
        order_processing_ratio_input = float(order_processing_ratio.get())
        packaging_ratio_input = float(packaging_ratio.get())
        PG_ratio_input = float(PG_ratio.get())
        delivery_morning_ratio_input = float(delivery_morning_ratio.get())
        delivery_3pl_ratio_input = float(delivery_3pl_ratio.get())
        delivery_morning_cj_ratio_input = float(delivery_morning_cj_ratio.get())
        dawn_choongchung_cost_input = float(dawn_choongchung_cost.get())
        dawn_buul_cost_input = float(dawn_buul_cost.get())


        root.quit()

        global params
        params = {
            "gmv": gmv_input,
            "orders": orders_input,
            "purchasers": purchasers_input,
            "cogs_ratio": cogs_ratio_input,
            "order_processing_ratio": order_processing_ratio_input,
            "packaging_ratio": packaging_ratio_input,
            "PG_ratio": PG_ratio_input,
            "delivery_morning_ratio": delivery_morning_ratio_input,
            "delivery_3pl_ratio": delivery_3pl_ratio_input,
            "delivery_morning_cj_ratio": delivery_morning_cj_ratio_input,
            "dawn_choongchung_cost": dawn_choongchung_cost_input,
            "dawn_buul_cost": dawn_buul_cost_input
        }

    root = tk.Tk()
    root.title("유지보수 : by jaesang.lee_b@kurlycorp.com")
    root.geometry("640x480+100+100")
    root.resizable(False, False)

    tk.Label(root, text = "1. GMV를 입력하세요", fg="black").grid(row = 0, sticky = "W")
    tk.Label(root, text = "2. Orders를 입력하세요", fg="black").grid(row = 1, sticky = "W")
    tk.Label(root, text = "3. Purchasers를 입력하세요", fg="black").grid(row = 2, sticky = "W")
    tk.Label(root, text="4. cogs_ratio를 입력하세요", fg="black").grid(row=3, sticky="W")
    tk.Label(root, text="5. order_processing_ratio를 입력하세요", fg="black").grid(row=4, sticky="W")
    tk.Label(root, text="6. packaging_ratio를 입력하세요", fg="black").grid(row=5, sticky="W")
    tk.Label(root, text="7. PG_ratio를 입력하세요", fg="black").grid(row=6, sticky="W")
    tk.Label(root, text="8. delivery_morning_ratio를 입력하세요", fg="black").grid(row=7, sticky="W")
    tk.Label(root, text="9. delivery_3pl_ratio를 입력하세요", fg="black").grid(row=8, sticky="W")
    tk.Label(root, text="10. delivery_morning_cj_ratio를 입력하세요", fg="black").grid(row=9, sticky="W")
    tk.Label(root, text="11. dawn_choongchung_cost를 입력하세요", fg="black").grid(row=10, sticky="W")
    tk.Label(root, text="12. dawn_buul_cost를 입력하세요", fg="black").grid(row=11, sticky="W")


    gmv = tk.Entry(root)
    orders = tk.Entry(root)
    purchasers = tk.Entry(root)
    cogs_ratio = tk.Entry(root)
    order_processing_ratio = tk.Entry(root)
    packaging_ratio = tk.Entry(root)
    PG_ratio = tk.Entry(root)
    delivery_morning_ratio = tk.Entry(root)
    delivery_3pl_ratio = tk.Entry(root)
    delivery_morning_cj_ratio = tk.Entry(root)
    dawn_choongchung_cost = tk.Entry(root)
    dawn_buul_cost = tk.Entry(root)


    gmv.grid(row=0, column=1, sticky="W")
    orders.grid(row=1, column=1, sticky="W")
    purchasers.grid(row=2, column=1, sticky="W")
    cogs_ratio.grid(row=3, column=1, sticky="W")
    order_processing_ratio.grid(row=4, column=1, sticky="W")
    packaging_ratio.grid(row=5, column=1, sticky="W")
    PG_ratio.grid(row=6, column=1, sticky="W")
    delivery_morning_ratio.grid(row=7, column=1, sticky="W")
    delivery_3pl_ratio.grid(row=8, column=1, sticky="W")
    delivery_morning_cj_ratio.grid(row=9, column=1, sticky="W")
    dawn_choongchung_cost.grid(row=10, column=1, sticky="W")
    dawn_buul_cost.grid(row=11, column=1, sticky="W")


    tk.Label(root, text = "입력된 변수는 params 변수로 저장됩니다.", fg="black", relief="ridge").grid(row = 12, sticky = "W")
    tk.Button(root, text = "SUBMIT", fg="gray", command = get_input).grid(row = 13, sticky = "W")

    global params

    tk.mainloop()