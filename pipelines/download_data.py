"""
下载 Kaggle 巴西电商数据集
数据集：olistbr/brazilian-ecommerce
包含：约 10 万订单，2016-2018 年，字段非常丰富

使用前提：
1. 注册 Kaggle 账号：https://www.kaggle.com
2. 进入 Account -> API -> Create New Token，下载 kaggle.json
3. 将 kaggle.json 放到项目根目录
"""

import os
import zipfile
import shutil

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')


def download_dataset():
    """下载 Kaggle 数据集"""
    os.makedirs(DATA_DIR, exist_ok=True)

    # 把 kaggle.json 复制到默认位置（~/.kaggle/）
    kaggle_json = os.path.join(os.path.dirname(__file__), '..', 'kaggle.json')
    kaggle_dir = os.path.expanduser('~/.kaggle')
    os.makedirs(kaggle_dir, exist_ok=True)

    if os.path.exists(kaggle_json):
        shutil.copy(kaggle_json, os.path.join(kaggle_dir, 'kaggle.json'))
        os.chmod(os.path.join(kaggle_dir, 'kaggle.json'), 0o600)
        print("✅ kaggle.json 已配置")
    else:
        print("⚠️  未找到 kaggle.json，请先下载并放到项目根目录")
        print("   下载地址：https://www.kaggle.com -> Account -> API -> Create New Token")
        return False

    try:
        import kaggle
        print("📥 开始下载数据集（约 40MB）...")
        kaggle.api.dataset_download_files(
            'olistbr/brazilian-ecommerce',
            path=DATA_DIR,
            unzip=True
        )
        print(f"✅ 数据集下载完成，保存到：{DATA_DIR}")
        return True
    except Exception as e:
        print(f"❌ 下载失败：{e}")
        print("   请手动下载：https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        return False


def verify_files():
    """验证文件是否齐全"""
    required_files = [
        'olist_orders_dataset.csv',
        'olist_order_items_dataset.csv',
        'olist_customers_dataset.csv',
        'olist_products_dataset.csv',
    ]
    missing = []
    for f in required_files:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024
            print(f"  ✅ {f} ({size:.0f} KB)")
        else:
            missing.append(f)
            print(f"  ❌ {f} - 缺失")

    if missing:
        print(f"\n⚠️  缺少 {len(missing)} 个文件，请检查下载")
        return False
    print("\n✅ 所有文件验证通过")
    return True


if __name__ == '__main__':
    print("=" * 50)
    print("  AI 数仓 - 数据集下载工具")
    print("=" * 50)

    # 检查是否已有数据
    if os.path.exists(os.path.join(DATA_DIR, 'olist_orders_dataset.csv')):
        print("📂 数据文件已存在，跳过下载")
    else:
        download_dataset()

    print("\n📋 文件验证：")
    verify_files()
