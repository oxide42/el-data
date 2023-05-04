import json
import hashlib

"""
I follow these steps when modeling using LSTM.
    1. Try a single hidden layer with 2 1r 3 memory cells. See how it performs against a benchmark.
       If it is a time series problem, then I generally make a forecast from classical time series
       techniques as benchmark.
    2. Try and increase the number of memory cells. If the performance is not increasing much then
       move on to the next step.
1  3. Start making the network deeper, i.e. add another layer with a small number of memory cells.

    1 output, 5000 samples
    1 layer: 245 neurons
    2 layers: 204, 40

    First shot, 1 layer model:
        one-layer-neurons = 2n+1, where n = input nodes
    Two layer model:
        first = one_layer_neurons * 0.83
        second = first * 0.16

"""


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class cfg(metaclass=Singleton):
    def __init__(self):
        self.USE_CACHED_DATA = True
        self.ONLY_RECENT_DATA = False
        self.CONTINUE_TRAINING = True
        self.SAVE_WEIGHTS = True

        self.EPOCHS = 1000
        self.LEARNING_RATE = 'AUTO'
        self.HIDDEN_LAYERS_NEURONS = [8, 10]
        self.DENSE_LAYER_NEURONS = 6
        self.DROPOUT = 0.05
        '''
        12-10-(6)-Dr0.05: loss=0.028
        12-6-(6)-Dr0.05: loss=0.028
        10-10-(8)-Dr0.05: loss=0.028
        10-10-(6)-Dr0.05: loss=0.028
        8-10-(6)-Dr0.05: params=1704, lr=1.5e-4, loss=0.028 <<<
        8-8-(8)-Dr0.05: loss=0.028
        8-6-(6)-Dr0.05: loss=0.028
        '''
        self.BIDIRECTIONAL = False
        self.KERNEL_REGULARIZATION = 0

        self.SPECIALITY = False

        self.AUTO_TRAIN_STOP = True
        self.AUTO_TRAIN_STOP_THRESHOLD = 200

        self.STATEFUL_TRAINING = True
        self.TRAINING_TIME_STEPS = 1
        self.TRAINING_BATCH_SIZE = 64

        self.STATEFUL_PREDICTION = True
        self.PREDICTION_TIME_STEPS = 1
        self.PREDICTION_BATCH_SIZE = 1
        self.FORECAST_HOURS = 96

        self.SHUFFLE_DATA = True
        self.TRAIN_TEST_SPLIT_PCT = 0.7
        self.VALIDATE_SIZE = 2000
        self.WARMUP_SIZE = 5 * self.TRAINING_BATCH_SIZE
        self.VALIDATE_FROM = self.VALIDATE_SIZE // self.TRAINING_BATCH_SIZE * self.TRAINING_BATCH_SIZE
        self.WARMUP_FROM = self.VALIDATE_FROM + self.WARMUP_SIZE
        self.FIT_VERBOSITY = 1

        self.FEATURES = [
            "DSin", "DCos",
            "YSin", "YCos",
            "w1", "w2", "w5", "w6", "w8", "w9",  # "w7", "w3", "w4",
            # "wa", "wb",
            "wc", "wd",
            "Temperature",
            # "Clouds",
            "SolarInsolation",
            # "Water",
            "GasPrice",
            "CoalPrice",
        ]

        self.NON_FORECASTED_FEATURES = [
            "GasPrice",
            "Water",
            "CoalPrice"
        ]

        self.OUTPUT = [
            "SpotPriceDK1",
            "SpotPriceDK2",
        ]

        self.PREDICTION = [
            "PredictionDK1",
            "PredictionDK2",
        ]

        self.PLOT_SERIES = [
            {'label': 'SpotPriceDK1', 'color': 'white', 'secondary': False},
            {'label': 'SpotPriceDK2', 'color': 'gray', 'secondary': False},
            {'label': 'GasPrice', 'color': 'red', 'secondary': True},
            {'label': 'CoalPrice', 'color': 'gray', 'secondary': True},
            {'label': 'PredictionDK1', 'color': 'orange', 'secondary': False},
            {'label': 'PredictionDK2', 'color': 'yellow', 'secondary': False},
        ]

        self.PLOT_SERIES_Y = 'date'
        self.PLOT_LABEL_X = 'time'
        self.PLOT_LABEL_Y = 'kr/kWh'

        self.FORECAST_HIDE_PREDICTION_WHERE_PRICE_IS_KNOWN = False

        self.DMI_API_KEY = '33262e76-8ef3-4061-b309-fc77150e082b'
        self.OPENWEATHERMAP_API_KEY = '9d24e778a4f529f23bea024c271872ce'

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

    def config_id(self) -> str:
        config_str = str(self.TRAINING_BATCH_SIZE) + \
                     str(self.OUTPUT) + \
                     str(self.STATEFUL_TRAINING) + \
                     str(self.TRAINING_TIME_STEPS) + \
                     str(self.FEATURES) + \
                     str(self.TRAIN_TEST_SPLIT_PCT)
        hashed = hashlib.md5(config_str.encode('utf-8')).hexdigest()[:6]
        return hashed
