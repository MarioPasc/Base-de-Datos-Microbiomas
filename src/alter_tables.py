import mysql.connector as mysql


class AlterTables:
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database


    def __connection__(self):
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=self.password,
                    database= self.database)
            return  connection
        except mysql.Error as err:
            print(f"Error: {err}")


    def __add_index__(self, table:str, column: str):
        connection= self.__connection__()
        cursor=connection.cursor()
        try:
            index_name=f'index_{"".join(col[0].upper() for col in column)}'
            line=f'ALTER TABLE `{table}` ADD INDEX `{index_name}` ({", ".join([f"`{col}`" for col in column])}) VISIBLE;'
            cursor.execute(line)
            connection.commit()
            print("ok")
            
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def __drop_index__(self, table: str, column:str):
        try:
            connection = self.__connection__()
            with connection.cursor() as cursor:
                index_name=f'index_{"".join(col[0].upper() for col in column)}'
                query = f'ALTER TABLE `{table}` DROP INDEX `{index_name}`;'
                cursor.execute(query)
                connection.commit()
                print("ok")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection:
                connection.close()

    def __alter_type__(self, table, column, new_type):
        try:
            connection = self.__connection__()
            with connection.cursor() as cursor:
                query = f'ALTER TABLE `{table}` CHANGE COLUMN `{column}` `{column}` {new_type} NULL DEFAULT NULL;'
                cursor.execute(query)
                connection.commit()
                print("ok")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection:
                connection.close()

    def __change_Storage__(self, table,type):
        try:
            connection = self.__connection__()
            with connection.cursor() as cursor:
                query =         line= f"ALTER TABLE {table} ENGINE={type}"
                cursor.execute(query)
                connection.commit()
                print("ok")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection:
                connection.close()


