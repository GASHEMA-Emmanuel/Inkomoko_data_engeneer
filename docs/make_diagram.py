#!/usr/bin/env python3
"""Render docs/architecture.png — the pipeline architecture diagram."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# Inkomoko-ish palette
NAVY = "#13235B"
BLUE = "#2BA9E0"
ORANGE = "#F26A3F"
LIGHT = "#EaF3Fb"
GREY = "#5b6472"
WHITE = "#ffffff"

fig, ax = plt.subplots(figsize=(13, 7.2))
ax.set_xlim(0, 130)
ax.set_ylim(0, 72)
ax.axis("off")


def box(x, y, w, h, title, subtitle="", fc=WHITE, ec=NAVY, tc=NAVY, lw=2):
    ax.add_patch(
        FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.6,rounding_size=2.2",
            linewidth=lw, edgecolor=ec, facecolor=fc, zorder=2,
        )
    )
    ax.text(x + w / 2, y + h / 2 + (1.6 if subtitle else 0), title,
            ha="center", va="center", fontsize=11, fontweight="bold",
            color=tc, zorder=3)
    if subtitle:
        ax.text(x + w / 2, y + h / 2 - 2.4, subtitle, ha="center", va="center",
                fontsize=8.2, color=tc, zorder=3)


def arrow(x1, y1, x2, y2, color=ORANGE, style="-|>", lw=2.4):
    ax.add_patch(FancyArrowPatch(
        (x1, y1), (x2, y2), arrowstyle=style, mutation_scale=18,
        linewidth=lw, color=color, zorder=1,
        connectionstyle="arc3,rad=0"))


# Title
ax.text(65, 69, "Inkomoko — End-to-End Analytics Engineering Pipeline",
        ha="center", va="center", fontsize=15, fontweight="bold", color=NAVY)

# Docker boundary
ax.add_patch(FancyBboxPatch(
    (2, 6), 126, 52, boxstyle="round,pad=0.4,rounding_size=2",
    linewidth=2, edgecolor=BLUE, facecolor="#f7fbfe", linestyle=(0, (6, 4)),
    zorder=0))
ax.text(7.5, 55.5, "Docker Compose", ha="left", va="center", fontsize=10,
        fontweight="bold", color=BLUE)

# Row of main stages (y ~ 30)
y = 28
h = 13
box(4, y, 18, h, "Public REST API", "JSONPlaceholder\n/users /posts /comments",
    fc=LIGHT, ec=GREY, tc=NAVY)
box(28, y, 18, h, "Ingestion", "Python • requests\nUPSERT into Postgres", fc=WHITE)
box(52, y, 18, h, "PostgreSQL", "OLTP • schema: raw\nusers/posts/comments", fc=WHITE, ec=NAVY)
box(76, y, 18, h, "Replication", "Python • incremental\nwatermark + validate", fc=WHITE)
box(100, y, 24, h, "ClickHouse", "OLAP • MergeTree\nraw → analytics", fc=WHITE, ec=ORANGE, tc=NAVY)

# arrows between stages
arrow(22, y + h / 2, 28, y + h / 2)
arrow(46, y + h / 2, 52, y + h / 2)
arrow(70, y + h / 2, 76, y + h / 2)
arrow(94, y + h / 2, 100, y + h / 2)

# dbt block (below ClickHouse)
box(100, 9, 24, 13, "dbt", "staging (views)\nmarts (tables) + tests", fc=LIGHT, ec=ORANGE, tc=NAVY)
arrow(112, y, 112, 22, color=ORANGE)
ax.text(118.5, 25, "transform", ha="center", va="center", fontsize=7.5,
        color=ORANGE, rotation=90)

# Airflow orchestration band (top)
box(28, 44, 96, 9, "Apache Airflow  (LocalExecutor)",
    "ingest_users / posts / comments → replicate → dbt run → dbt test", fc=NAVY,
    ec=NAVY, tc=WHITE, lw=2)
# dashed orchestration arrows down to each stage
for cx in (37, 61, 85, 112):
    ax.add_patch(FancyArrowPatch((cx, 44), (cx, y + h),
                 arrowstyle="-|>", mutation_scale=12, linewidth=1.4,
                 color=BLUE, linestyle=(0, (3, 2)), zorder=1))

# Consumers
box(4, 9, 18, 13, "Consumers", "BI / ML / DBeaver\n(analytics marts)", fc=LIGHT,
    ec=GREY, tc=NAVY)
arrow(100, 12, 22, 12, color=NAVY, lw=1.8)

fig.tight_layout()
fig.savefig("docs/architecture.png", dpi=160, bbox_inches="tight",
            facecolor="white")
print("Wrote docs/architecture.png")
