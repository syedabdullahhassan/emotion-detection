import numpy as np
import pandas as pd
import  yaml

import os
import logging

#configure logging
logger=logging.getLogger('data_igestion')
logger.setLevel('DEBUG')
console_handler=logging.Streamandler()
console_handler.setLevel('DEBUG')

file_handler= logging.FileHandler('erros.log')

file_handler.setLevel('Error')


formatter=logging.Formatter('%(asctime)s -%(name)s -%(levelname)s -%(massage)s')

console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


from sklearn.model_selection import train_test_split

def load_params(params_path: str) -> float:
    try:
        with open('params_path','r') as file:
            params=yaml.safe(file)
        test_size= params['data_ingestion']['test_size']
        return test_size
    except FileNotFoundError:
        logger.error('file not found')
        raise
    except yaml.YamlError as e:
        logger.error('Yaml error')
        raise
    except Exeception as e:
        logger.error('some error occured')

def load_data(url: str) -> pd.DataFrame:

    df=pd.read_csv(url)
    return df
# delete tweet id
def process_data(df: pd.DataFrame)-> pd.DataFrame:
    df.drop(columns=['tweet_id'],inplace=True)
    final_df=df[df['sentiment'].isin(['happiness','sadness'])]
    final_df['sentiment'].replace({'happiness':1,'sadness':0},inplace=True)
    return final_df


def save_data(data_path: str,train_data: pd.DataFrame,test_data:pd.DataFrame)-> None:

    
    os.makedirs(data_path)
    train_data.to_csv(os.path.join(data_path,"train.csv"))
    test_data.to_csv(os.path.join(data_path,"test.csv"))

def main():
    test_size=load_params('params.yaml')
    df=load_data('https://raw.githubusercontent.com/campusx-official/jupyter-masterclass/main/tweet_emotions.csv')
    final_df= process_data(df)

   
    train_data,test_data= train_test_split(final_df,test_size=test_size ,random_state=42)

    data_path= os.path.join("data","raw")
    save_data(data_path,train_data,test_data)

if __name__ =="__main__":
    main()

