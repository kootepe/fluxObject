import os
import logging
from pathlib import Path

import openpyxl as opxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

logger = logging.getLogger("defaultLogger")


def create_excel(df, name, path):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Fluxes"
    ws1 = wb.worksheets[0]

    num = 1
    ws1.column_dimensions["A"].width = 13
    for r in dataframe_to_rows(df, index=False, header=True):
        ws1.append(r)
        ws1.row_dimensions[num].height = 20
        num += 1

    o = 1
    plot_dirs = [d for d in Path("figs/").iterdir() if d.is_dir()]
    for idx, col in enumerate(plot_dirs):
        ws1.insert_cols(1)
        # ws1.column_dimensions[get_column_letter(idx)].width = 13
    for dir in plot_dirs:
        # ws1.insert_cols(1)
        ws1.column_dimensions[get_column_letter(o)].width = 13
        ws1.cell(row=1, column=o).value = f"{dir.name}_graph"
        row = 2
        # plotDir = f"figs/{dir}/{name}"
        plot_dir = dir / name
        plots = [f for f in plot_dir.iterdir() if f.is_file()]
        plots = sorted(plots)
        for j in range(len(plots)):
            anchorString = get_column_letter(o) + str(row)
            # img = opxl.drawing.image.Image(f"{plotDir}/{plots[j]}")
            img = opxl.drawing.image.Image(f"{plots[j]}")
            # cell = ws1.cell(row=row, column=o)
            ws1.add_image(img, anchorString)
            # ws1.add_image(img)
            row += 1
        o += 1
    exists = os.path.exists(path)
    if not exists:
        os.makedirs(path)
    # NOTE: CHECK IF FILE EXISTS AND USE ANOTHER NAME IF IT DOES
    xlsxName = f"{path}/{name}.xlsx"
    wb.save(xlsxName)


def create_sparkline(df, filename, gas, fig, ax):
    """
    Create a sparkline (small graph) that's on each row of the output excel

    Parameters
    ----------
    df : pd.DataFrame
    filename : datetime.datetime
    """
    day = str(filename.date())
    name = filename.strftime("%Y%m%d%H%M%S")
    data = df[gas].tolist()
    path = f"figs/{gas}/{day}/"
    plotname = f"{name}.png"
    exists = os.path.exists(path)
    if not exists:
        os.makedirs(path)

    ax.plot(data, color="g", linewidth=0.3)
    ax.set_xticks([], [])
    ax.set_yticks([], [])
    canvas = FigureCanvas(fig)
    canvas.print_figure(f"{path}{plotname}", dpi=150,
                bbox_inches="tight", pad_inches=0.005)
    ax.cla()


def create_fig():
    fig = Figure(figsize=(0.8, 0.20))
    ax = fig.add_subplot()
    ax.set_frame_on(False)
    ax.set_xticks([], [])
    ax.set_yticks([], [])

    return fig, ax
