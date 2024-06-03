from mysql_queries import Queries


def main():
    
    myqueries = Queries(password="bdbiO", database="microbiomeDB")
    myqueries.query1()
    myqueries.query2("MIC-17098-ZUZ") #enter the desire microorganism ID
    myqueries.query3("Tuberculosis") #enter a disease
    myqueries.query4()
    myqueries.query5()
    myqueries.query6()
    myqueries.query7()

if __name__ == "__main__":
    main()