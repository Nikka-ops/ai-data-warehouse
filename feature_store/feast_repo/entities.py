# Feast 实体定义 — 对应业务中的主键维度
try:
    from feast import Entity
    from feast.value_type import ValueType
except ImportError:
    import warnings
    warnings.warn("feast 未安装，entities.py 仅作占位，请执行 pip install feast", stacklevel=1)
    Entity = None  # type: ignore

if Entity is not None:
    # 用户实体
    user_entity = Entity(
        name="user_id",
        description="用户唯一标识",
    )

    # 卖家实体
    seller_entity = Entity(
        name="seller_id",
        description="卖家唯一标识",
    )

    # 商品类目实体
    category_entity = Entity(
        name="category",
        description="商品类目",
    )
else:
    user_entity = None  # type: ignore
    seller_entity = None  # type: ignore
    category_entity = None  # type: ignore
