import pandas as pd

df = pd.read_csv("titanic.csv")
print(df.head())

df.info()

Sex_id_not_null = df.Sex.notnull()
dq_Sex_id = Sex_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_Sex_id}")

Name_id_not_null = df.Name.notnull()
dq_Name_id = Name_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_Name_id}")

Pclass_id_not_null = df.Pclass.notnull()
dq_Pclass_id = Pclass_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_Pclass_id}")

Survived_id_not_null = df.Survived.notnull()
dq_Survived = Survived_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_Survived}")

passenger_id_not_null = df.PassengerId.notnull()
dq_passenger_id = passenger_id_not_null.sum() / len(df)
print(f"Data Quality of PassengerId: {dq_passenger_id}")

age_not_null = df.Age.notnull()
dq_age = age_not_null.sum() / len(df)
print(f"Data Quality of Age: {dq_age}")

cabin_not_null = df.Cabin.notnull()
dq_cabin = cabin_not_null.sum() / len(df)
print(f"Data Quality of Cabin: {dq_cabin}")

embarked_not_null = df.Embarked.notnull()
dq_embarked = embarked_not_null.sum() / len(df)
print(f"Data Quality of Embarked: {dq_embarked}")

# แก้ไขตัวแปรให้ตรงกัน
print(f"Completeness: {(dq_Sex_id + dq_Name_id + dq_Pclass_id + dq_Survived + dq_passenger_id + dq_age + dq_cabin + dq_embarked) / 8}")
