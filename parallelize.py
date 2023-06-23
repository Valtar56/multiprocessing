import pandas as pd
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool, Pipe, Process

class Parallelize:
    def __init__(self):
        pass

    def target_function(self, tuple_input):
        col_1_obj, col_2_obj, col_3_obj = tuple_input
        output = col_1_obj * col_2_obj * col_3_obj # just for represesntation purpose
        return output

    def parallel_column_generation(self, df, connection):
        input_for_target = list(zip(df['col_1'], df['col_2'], df['col_3']))
        try:
            new_df=pd.DataFrame()
            new_df['col_4']= pd.Series(map(self.target_function, input_for_target))            
            connection.send([new_df])
            connection.close()
        except Exception as ex:
            print(ex)
            connection.send([])
            connection.close()

    def parallelize(self, data = None, partitions = None, target = None):
        data_split=np.array_split(data, partitions)

        parent_connections=[]
        processes=[]

        for i in range(len(data_split)):
            parent_conn, child_conn= Pipe()
            parent_connections.append(parent_conn)
            process=Process(target=target, args=(data_split[i],child_conn,))
            processes.append(process)

        pcount=1
        for process in processes:
            # dlogger.info("Process Launched: ",pcount)
            pcount+=1
            process.start()
        
        print("All process launched")
        
        num=1   
        df_result_temp=pd.DataFrame()
        for parent_connection in parent_connections:
            df_result=parent_connection.recv()[0]
            print("Process completed: ", num)
            df_result_temp=pd.concat([df_result,df_result_temp],axis=0)
            num+=1
        
        for process in processes:
            process.join()
        
        df_result = df_result_temp.reset_index(drop = True)

        # dlogger.info("All process joined")
        return df_result
    
    def main(self, df):
        partitions = mp.cpu_count()-2
        df_with_added_col = self.parallelize(data = df, partitions = partitions, target = self.parallel_column_generation)
        return df_with_added_col
    
if __name__ == "__main__":
    df = pd.read_csv('path_to_csv_file.csv')
    parallelize_obj = Parallelize()
    parallelize_obj.main(df = df)


