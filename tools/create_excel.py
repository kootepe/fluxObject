import openpyxl as opxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
import matplotlib.pyplot as plt
import os
import logging

logging = logging.getLogger("__main__")


def create_excel(df, name):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Fluxes"

    num = 1
    ws1.column_dimensions["A"].width=13
    for r in dataframe_to_rows(df, index=False, header=True):
        ws1.append(r)
        ws1.row_dimensions[num].height=20
        num += 1

    ws1.insert_cols(1)
    row = 2
    plotDir = f"figs/{name}/"
    plots = os.listdir(plotDir)
    print(plots)
    plots = sorted(plots)
    print(plots)
    for j in range(len(plots)):
        anchorString = "A" + str(row)
        # img = opxl.drawing.image.Image(plotDir / plots[j])
        img = opxl.drawing.image.Image(f"{plotDir}/{plots[j]}")
        ws1.add_image(img, anchorString)
        row += 1
    # if mode == "ac":
    #     xlsxName = "fluxSummaryAC.xlsx"
    # if mode == "man":
    #     xlsxName = "fluxSummaryManual.xlsx"
    xlsxName = f"{name}.xlsx"
    wb.save(xlsxName)

def create_sparkline(df, filename):
    day = str(filename.date())
    fig = plt.figure()
    fig, ax = plt.subplots(figsize=(0.8, 0.20))

    # for _, p in allRaw.groupby(["date", "startTime"]):
    name = filename.strftime("%Y%m%d%H%M%S")
    data = df["ch4"].tolist()
    path = f"figs/{day}/"
    plotname = f"{name}.jpg"
    exists = os.path.exists(path)
    if not exists:
        os.makedirs(path)

    ax.plot(data, color="g", linewidth=0.3)
    ax.set_frame_on(False)
    plt.tick_params(
        axis="both", which="both", bottom=False, top=False, labelbottom=False
    )
    ax.set_xticks([], [])
    ax.set_yticks([], [])
    fig.savefig(f"{path}{plotname}", dpi=150, bbox_inches="tight", pad_inches=0.005)
    ax.cla()
    plt.close(fig)
