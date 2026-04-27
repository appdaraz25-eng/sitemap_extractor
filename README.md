# 📰 Sitemap Extractor — GitHub Auto-Run Setup

## 📁 Folder Structure (DO NOT CHANGE)
```
your-repo/
├── sitemap_extractor.py          ← Main script
├── sites.txt                     ← Your website URLs go here
└── .github/
    └── workflows/
        └── daily_extraction.yml  ← Auto-run schedule (DO NOT EDIT)
```

---

## 🚀 How to Upload to GitHub (3 Steps)

### Step 1 — Create a new GitHub repository
1. Go to https://github.com
2. Click the **"+"** button (top right) → **"New repository"**
3. Name it anything e.g. `sitemap-extractor`
4. Set to **Private** (recommended)
5. Click **"Create repository"**

### Step 2 — Upload ALL files
1. In your new repo, click **"uploading an existing file"**
2. Drag and drop ALL files AND the `.github` folder together
   - `sitemap_extractor.py`
   - `sites.txt`
   - `.github/workflows/daily_extraction.yml`
3. Click **"Commit changes"**

> ⚠️ IMPORTANT: The `.github` folder may be hidden on Windows.
> To see it: Open File Explorer → View → Check "Hidden items"

### Step 3 — Enable GitHub Actions
1. Click the **"Actions"** tab in your repo
2. If prompted, click **"I understand my workflows, go ahead and enable them"**
3. ✅ Done! It will now run every day at **6:00 AM Bangladesh time**

---

## ✏️ How to Edit Your Sites

Open `sites.txt` on GitHub:
1. Click `sites.txt` in your repo
2. Click the ✏️ pencil icon to edit
3. Add/remove URLs (one per line)
4. Click **"Commit changes"**

---

## ▶️ How to Run Manually (Any Time)

1. Go to **Actions** tab
2. Click **"Daily Sitemap Extraction"** on the left
3. Click **"Run workflow"** → **"Run workflow"**

---

## 📥 Where Are My Output Files?

After each run:
- **`.xlsx` and `.db` files** are automatically saved back into your repo
- Also available under **Actions → latest run → Artifacts** (kept 30 days)

---

## 🕐 Schedule

| Time Zone | Time |
|-----------|------|
| Bangladesh (BST) | 6:00 AM daily |
| UTC | 12:00 AM (midnight) |

---

## 💻 Run on Your Own PC (GUI Mode)
```
pip install aiohttp pandas openpyxl beautifulsoup4 python-dateutil PyQt6 qasync requests validators lxml pytz tqdm
python sitemap_extractor.py
```
