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

## 🧪 Sample Log Format (JSON)

```json
{
  "stages": [
    {
      "stageId": 1,
      "numPartitions": 3,
      "partitionData": [
        { "key": "apple", "count": 10 },
        { "key": "banana", "count": 5 },
        ...
      ]
    }
  ]
}
