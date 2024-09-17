import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["mydatabase"]
collection = db["mycollection"]

# Read the CSV file into a pandas DataFrame
df = pd.read_csv("survey_results_public.csv")

collection = db["mycollection"]

#Convert the DataFrame to a list of dictionaries
#data = df.to_dict(orient="records")

#Insert the data into the MongoDB collection
#collection.insert_many(data)

try:
    client.admin.command('ping')
    print("You successfully connected to MongoDB!")
except Exception as e:
    print(e)

#print(df.columns) # print all the columns in the collection.

#Analysis 1: Most Popular programming languages among developer.

def perform_analysis_1():
    pipeline = [
    {"$match":{"LanguageHaveWorkedWith": {"$exists": True, "$ne": None}}},
    {"$unwind": "$LanguageHaveWorkedWith"},
    {"$group": {"_id": "$LanguageHaveWorkedWith", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
    ]

    results = list(collection.aggregate(pipeline))
    df = pd.DataFrame(results)
    df = pd.DataFrame(results).dropna().sort_values(by=['count'], ascending=False)

    x_values = list(df['_id'][:10])
    y_values = list(df['count'][:10])
   # Convert x-values to strings
    x_values = [str(x) for x in x_values]

# Plot the bar chart
    plt.bar(x_values, y_values)

    plt.xticks(rotation=90, fontsize=8)
    plt.xlabel('Programming Languages')
    plt.ylabel('Number of Developers')
    plt.title('Top 10 Most Popular Programming Languages')
    plt.show()
    return results

#Analysis 2 : Query to get the average hourly wage for carribean countries

def perform_analysis_2():
    pipeline = [
    {"$project": {"Country": 1, "ConvertedCompHourly": {"$cond": {"if": {"$gt": ["$ConvertedCompYearly", None]}, "then": {"$divide": ["$ConvertedCompYearly", 52*40]}, "else": None}}}},

    {"$group": {"_id": "$Country", "AvgHourlyWage": {"$avg": "$ConvertedCompHourly"}}},
    {"$sort": {"AvgHourlyWage": -1}}
    #{"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline)) 
    #print(df['Country'].unique())
    countries = [doc["_id"] for doc in results]
    wages = [doc["AvgHourlyWage"] for doc in results]
    # Create bar chart
    non_nan_wages = [wage for wage in wages if wage is not None and not np.isnan(wage)]
    non_nan_countries = [country for (country, wage) in zip(countries, wages) if wage is not None and not np.isnan(wage)]

    # Create pie chart
    fig, ax = plt.subplots()
    ax.pie(non_nan_wages, labels=non_nan_countries, autopct=lambda x: f"{x:.1f}" if not np.isnan(x) else 'N/A')

    ax.set_title("Top 4 Countries by Average Hourly Wage for Developers")
    ax.axis('equal')

    plt.show()
    return results


#Analysis 3 : Most popular databases among the developers

def perform_analysis_3():
    pipeline = [
    {"$match": {"DatabaseHaveWorkedWith": {"$exists": True, "$ne": None}}},
    {"$unwind": "$DatabaseHaveWorkedWith"},
    {"$group": {"_id": "$DatabaseHaveWorkedWith", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
    ]

    results = list(collection.aggregate(pipeline))

    df = pd.DataFrame(results).dropna()
# Create lists of x and y values
    x_values = list(df['_id'].head(10))
    y_values = list(df['count'].head(10))
# Convert x-values to strings
    x_values = [str(x) for x in x_values]

# Plot the bar
    plt.bar(x_values, y_values)

# rotate the X-axis labels by 90 degrees and reduce font size
    plt.xticks(rotation=90, fontsize=8)

    plt.xlabel('Database')
    plt.ylabel('Count')
    plt.title('Most Popular Databases Among Developers')
    plt.show()
    return results

#Analysis 4 : Calculate the percenatge of respondents from each country in the survey

def perform_analysis_4():
    pipeline = [
    {"$match": {"Country": {"$ne": None}}},
    {"$group": {"_id": "$Country", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    df_result = pd.DataFrame(result)

# Calculate the percentage of respondents by country
    df_result["percentage"] = (df_result["count"] / df_result["count"].sum() * 100)/(1.7)

# Plot the data
    plt.figure(figsize=(8, 6))
    plt.bar(df_result["_id"], df_result["percentage"], color="green")
    plt.xticks(rotation=45, ha="right")
    #plt.bar(df["_id"], df["percentage"], color="green")
    plt.title("Top 10 Countries by Number of Respondents")
    plt.xlabel("Country")
    plt.ylabel("Percentage of Respondents")
    plt.show()
    return result

while True:
    print("What analysis would you like to perform?")
    print("1. Analysis 1")
    print("2. Analysis 2")
    print("3. Analysis 3")
    print("4. Analysis 4")
    choice = input("Enter your choice (1, 2, 3 or 4): ")

    if choice == "1":
        results = perform_analysis_1()
        #print(results)
    
    elif choice == "2":
        results = perform_analysis_2()
        #print(results)
    
    elif choice == "3":
        results = perform_analysis_3()
        #print(results)

    elif choice == "4":
        results = perform_analysis_4()
        #print(results)
    
    else:
      print("Invalid choice. Please enter 1, 2, 3 or 4.")