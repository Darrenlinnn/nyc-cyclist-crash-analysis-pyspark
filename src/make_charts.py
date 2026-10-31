"""Turn the Spark output tables into README charts."""
import glob
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

OUT, IMG = sys.argv[1], sys.argv[2]
read = lambda n: pd.read_csv(glob.glob(f"{OUT}/{n}_csv/*.csv")[0])
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False})

# 1) Hour x weekday heatmap
hw = read("by_hour_weekday")
days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
grid = hw.pivot(index="weekday", columns="hour", values="crashes").reindex(days).fillna(0)
fig, ax = plt.subplots(figsize=(11, 3.6), dpi=160)
im = ax.imshow(grid.values, aspect="auto", cmap="Reds")
ax.set_yticks(range(7), days); ax.set_xticks(range(0, 24, 2), [f"{h}:00" for h in range(0, 24, 2)])
ax.set_title("When cyclists get hurt: crashes by hour and weekday (NYC, 2020–2025)", loc="left")
fig.colorbar(im, ax=ax, label="Crashes", fraction=0.025)
for s in ax.spines.values(): s.set_visible(False)
fig.tight_layout(); fig.savefig(f"{IMG}/hour_weekday_heatmap.png")

# 2) Top 5 streets per borough
ts = read("top_streets_by_borough")
order = read("by_borough")["borough"].tolist()
fig, axes = plt.subplots(1, len(order), figsize=(14, 3.6), dpi=160, sharex=False)
for ax, b in zip(axes, order):
    d = ts[ts.borough == b].sort_values("crashes")
    ax.barh(d.street.str.title(), d.crashes, color="#c0392b")
    ax.set_title(b, fontsize=11, fontweight="bold"); ax.tick_params(axis="y", labelsize=8)
    for y, v in enumerate(d.crashes): ax.text(v, y, f" {v}", va="center", fontsize=8)
fig.suptitle("Top 5 streets for cyclist injury crashes in each borough (2020–2025)", x=0.01, ha="left")
fig.tight_layout(); fig.savefig(f"{IMG}/top_streets_by_borough.png")
print(grid.stack().idxmax(), int(grid.values.max()))
