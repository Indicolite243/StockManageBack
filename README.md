# 后端最简部署说明

## 1. 创建并进入 Python 环境

```powershell
conda create --name myprojectenv python=3.11
conda activate myprojectenv
```

## 2. 安装依赖

```powershell
pip install -r requirements.txt
```

## 3. 安装并启动本地 MongoDB

本项目默认使用本地 MongoDB：

- 地址：`mongodb://127.0.0.1:27017/`
- 数据库名：`stock_manager_local`

确认 MongoDB 服务已经启动后，在 **cmd** 中执行下面这组命令：

```cmd
setx MONGODB_URI "mongodb://127.0.0.1:27017/"
setx MONGODB_DB_NAME "stock_manager_local"
mongosh "mongodb://127.0.0.1:27017" --eval "db = db.getSiblingDB('stock_manager_local'); db.createCollection('latest_account_state'); db.createCollection('account_snapshots_highfreq'); db.createCollection('account_snapshots'); db.createCollection('account_snapshots_daily'); print('MongoDB 初始化完成: stock_manager_local');"
```

## 4. 安装 QMT 后必须配置的数据目录

系统读取的不是 `QMT.exe` 路径，而是 **QMT 的 `userdata_mini` 目录**。

### 推荐做法：用 CMD 配环境变量

在 **cmd** 中执行：

```cmd
setx XT_USERDATA_PATH "D:\GuoJin_2nd\国金QMT交易端模拟\userdata_mini"
```

请把上面的路径替换成你自己新电脑上的真实 `userdata_mini` 路径。

### 如果你不想配环境变量，就直接改 settings.py

打开这个文件：

`StockManager_Backendcode\StockManager_Backendcode\settings.py`

找到这一行：

```python
'USERDATA_PATH': os.getenv('XT_USERDATA_PATH', r'D:\GuoJin_2nd\国金QMT交易端模拟\userdata_mini')
```

把里面默认的路径：

```text
D:\GuoJin_2nd\国金QMT交易端模拟\userdata_mini
```

替换成你新电脑上 QMT 的真实 `userdata_mini` 路径。

例如你的新电脑如果是：

```text
E:\QMT\userdata_mini
```

那就改成：

```python
'USERDATA_PATH': os.getenv('XT_USERDATA_PATH', r'E:\QMT\userdata_mini')
```

注意：

1. 这里填的是 `userdata_mini` 文件夹路径
2. 不是 QMT 安装目录
3. 不是 `QMT.exe` 文件路径

## 5. 重新打开一个新终端后执行

```powershell
conda activate myprojectenv
python manage.py check
python manage.py sync_qmt_snapshots
python manage.py runserver --noreload
```

## 6. 一定要满足的条件

1. MongoDB 已启动
2. QMT 已安装并登录
3. `XT_USERDATA_PATH` 或 `settings.py` 里的 `USERDATA_PATH` 指向正确的 `userdata_mini`

## 7. 常见失败原因

如果 `sync_qmt_snapshots` 失败，优先检查：

1. MongoDB 没启动
2. QMT 没登录
3. `XT_USERDATA_PATH` 或 `settings.py` 里的 `USERDATA_PATH` 填错了，不是 `userdata_mini` 目录
