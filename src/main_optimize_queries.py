import argparse
import queries
import pandas
import os
import alter_tables

def main():
    parser = argparse.ArgumentParser(description="Connect to a MySQL database using Python.")
    
    parser.add_argument('-p', '--password', 
                        type=str, help='MySQL Password', required=True)
    parser.add_argument('-d', '--database', 
                        type=str, help='Database name', required=True)
    args = parser.parse_args()
    
    myqueries = queries.Queries(password=args.password, 
                                  database=args.database)
    alter=alter_tables.AlterTables(password=args.password, 
                                  database=args.database)
    


    #find suitable and optimal index for each query.

    def time_query2():
        list_time=[]
    #obtein all the microorganism id
        csv = pandas.read_csv(os.path.join(os.getcwd(), "specification-files", "microorganisms.csv"))
        for _, row in csv.iterrows():
            t=0
        for _ in range(10):
            tiempo=myqueries.__query2__(row["Microorganism_ID"]) 
            t+=tiempo
        list_time.append(t/10)
        return sum(list_time)/len(list_time)

    print("Query 2")
    t1=time_query2()
    print("Initial time: ", str(t1))

    alter.__add_index__("sample_microorganism",["Microorganism_ID","qPCR"])
    t2=time_query2()
    print("Time with new index: ",str(t2))
    if(t2>t1):
        #drop index if the response time is worse
        alter.__drop_index__("sample_microorganism",["Microorganism_ID","qPCR"])
    
            

    def time_query3():
        diseases=["Tuberculosis", "Respiratory infections","Tetanus","Candidiasis","Typhoid fever","Urinary tract infections","Streptococcal pharyngitis","Hepatitis B",
"Hepatitis C","Chickenpox","Gonorrhea","Shigellosis","Skin infections","Pertussis","HIV/AIDS","Malaria","Cholera","HPV infections","Skin infections"]
        list_time=[]
    #obtein all the microorganism id
        for d in diseases:
            t=0
            for _ in range(10):
                tiempo=myqueries.__query3__(d) 
                t+=tiempo
            list_time.append(t/10)
        return sum(list_time)/len(list_time)


    print("Query 3")
    
    #change Disease type to VARCHAR(30), since is not possible to make a type TEXT index
    alter.__alter_type__("patient","Disease","VARCHAR (30)")
    t1=time_query3()
    print("Initial time: ", str(t1))



    alter.__add_index__("sample",["Patient_ID","date"])
    t2=time_query3()
    print("Index sample table: ", str(t2))
    #alter.__drop_index__("sample",["Patient_ID","date"])



    alter.__add_index__("patient",["Patient_ID","Disease"])
    t3=time_query3()
    print("Index patient table: ", str(t3))

    #alter.__add_index__("sample",["Patient_ID","date"])
    t3=time_query3()
    print("Index both tables: ", str(t3))

    def time_query4():
        time=0
        for _ in range(20):
            t=myqueries.__query4__() 
            time+=t
        return time/20

    print("Query 4")
    t1=time_query4()
    print("Initial time: ",str(t1))
    alter.__add_index__("sample",["Sample_Type"])
    t2=time_query4()
    print("Time with new index: ",str(t2))
    if(t2>t1):
        alter.__drop_index__("sample",["Sample_Type"])


    def time_query5():
        time=0
        for _ in range(20):
            t=myqueries.__query5__() 
            time+=t
        return time/20
 

    print("Query 5")
    t1=time_query5()
    print("Initial time: ", str(t1))
    alter.__add_index__("sample",["Sample_Type","Sample_ID"])
    t2=time_query5()
    print("Index sample table: ", str(t2))
    if(t2>t1):
        alter.__drop_index__("sample",["Sample_Type","Sample_ID"])


    
    def time_query7():
        time=0
        for _ in range(20):
            t=myqueries.__query7__() 
            time+=t
        return time/20

    print("Query 7")
    t1=time_query7()
    print("Initial time: ", str(t1))
    
    alter.__alter_type__("microorganism","Species","VARCHAR (50)")
    #change type of Species
   
    alter.__add_index__("microorganism",["Species"])
    t2=time_query7()
    print("Index sample table: ", str(t2))
    if(t2>t1):
        alter.__drop_index__("microorganism",["Species"])


if __name__ == "__main__":
    main()