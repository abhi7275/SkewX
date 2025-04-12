# ⚡ SkewX - Data Skew Optimizer

**SkewX** is a real-time Spark job analyzer that detects **data skew** using **Zipf's Law** and provides actionable **optimization suggestions**. It helps identify uneven data distributions that can slow down distributed systems — empowering data engineers with fast diagnostics and smarter tuning strategies.

> ⚙️ Built with Python · Streamlit · Matplotlib · Seaborn  
> 🔍 Designed for Spark + Big Data Environments

---

## 🔎 Problem it Solves

In distributed systems like Spark, **data skew** causes some partitions to be overloaded while others remain underutilized — leading to:
- Slower jobs ⏱️
- Inefficient resource usage 💸
- Pipeline failures 🔥

**SkewX** solves this by:
- Parsing Spark logs
- Analyzing key distributions
- Detecting skew using **Zipf coefficient**
- Recommending rebalancing strategies

---

## 🚀 Features

✅ Upload Spark logs in `.json`, `.csv`, or `.txt`  
✅ Detect data skew based on **Zipf distribution**  
✅ Visualize top keys and frequency heatmaps  
✅ Get **real optimization suggestions**  
✅ Download results as CSV  
✅ Clean, interactive dashboard powered by Streamlit

---

## 📸 Demo Screenshots

> _(Add screenshots or a GIF of your app here)_

---
![Screenshot_12-4-2025_19555_opulent-cod-4vqvq664vg2jqp6-8507 app github dev](https://github.com/user-attachments/assets/de443c58-408a-44c6-98f8-109b1e46707c)

![Screenshot 2025-04-12 195523](https://github.com/user-attachments/assets/28d4b948-e824-4960-bb9e-0e26fd6d395e)


