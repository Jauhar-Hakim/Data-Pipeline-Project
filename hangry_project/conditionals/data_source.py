if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(data, data_2, data_3, *args, **kwargs) -> bool:
    if data|data_2|data_3 is not None:
        return True