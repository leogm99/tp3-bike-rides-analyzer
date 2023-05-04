# Bike Rides Analyzer

## How to run

Download the data from https://www.kaggle.com/datasets/jeanmidev/public-bike-sharing-in-north-america and place it into a folder called `data` inside the `client` package.
The data folder should have the following structure:

```
client/data/
├── montreal
│   ├── stations.csv
│   ├── trips.csv
│   └── weather.csv
├── toronto
│   ├── stations.csv
│   ├── trips.csv
│   └── weather.csv
└── washington
    ├── stations.csv
    ├── trips.csv
    └── weather.csv

3 directories, 9 files
```

Once that is done, from the root folder you can call `make docker-compose-up` to bring the system up. To change the number of replicas you can also use the utility script located
at `scripts/generate_compose_file.py`. Once called, you generate a new docker-compose-file that can just be moved into the root directory. 


## Output

You will find the output at `client/output`.
