import pandas as pd
from sqlalchemy import create_engine
from google.cloud.sql.connector import Connector, IPTypes
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pickle

PROJECT_ID = "ds-561-mohitsai"
DB_USER = "root"
DB_PASS = "CloudComputing"
DB_NAME = "hw5-db"
INSTANCE_CONNECTION_NAME = "ds-561-mohitsai:us-central1:ds561-hw5"

print("Initializing connector...")
connector = Connector()

def getconn():
    print("Connecting to database...")
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        ip_type=IPTypes.PUBLIC
    )
    print("Connected to database.")
    return conn

print("Creating SQLAlchemy engine...")
engine = create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

print("Fetching data from the database...")
with engine.connect() as conn:
    query = "SELECT * FROM requests;"
    df = pd.read_sql(query, conn)
print(f"Data fetched. Shape of DataFrame: {df.shape}")

csv_file = "requests.csv"

df.to_csv(csv_file, index=False)
print(f"Data saved to {csv_file}")

print("Loading data from CSV...")
df = pd.read_csv(csv_file)
print(f"Data loaded successfully. Shape of DataFrame: {df.shape}")

print("Starting data preprocessing...")
# Encode client_ip and client_country for Model 1
ip_encoder = LabelEncoder()
country_encoder = LabelEncoder()
df['client_ip_encoded'] = ip_encoder.fit_transform(df['client_ip'])
df['country_encoded'] = country_encoder.fit_transform(df['client_country'])

# Convert 'request_time' to datetime and numeric format for Model 2
df['request_time'] = pd.to_datetime(df['request_time'])
df['request_time'] = df['request_time'].astype(int) // 10**9  # Convert to seconds since epoch

# Label encode other categorical variables for Model 2
label_encoders = {}
for column in ['client_country', 'gender', 'age', 'income', 'requested_file', 'client_ip']:
    le = LabelEncoder()
    df[column] = le.fit_transform(df[column])
    label_encoders[column] = le

print("Data preprocessing completed successfully.")
print("==========================================================\n")


print("Model 1: Predicting client country based on client IP")
X_country = df[['client_ip_encoded']]
y_country = df['country_encoded']

X_train_country, X_test_country, y_train_country, y_test_country = train_test_split(
    X_country, y_country, test_size=0.2, random_state=42
)

def evaluate_model(model, model_name, X_train, X_test, y_train, y_test, prediction_target, model_filename):
    print(f"Training {model_name}...")
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"{model_name} accuracy for predicting {prediction_target}: {accuracy * 100:.2f}%")

    # Save the model to a file
    with open(model_filename, 'wb') as model_file:
        pickle.dump(model, model_file)

    print("==========================================================\n")

rf_country = RandomForestClassifier(
    random_state=42, n_estimators=100, min_samples_split=5,
    min_samples_leaf=2, max_depth=30, bootstrap=False
)
evaluate_model(rf_country, "Random Forest (Model 1)", X_train_country, X_test_country, y_train_country, y_test_country, "country", "rf_country_model.pkl")


print("Model 2: Predicting income based on other fields")
X_income = df.drop(columns=['income'])  # Use all columns except 'income' as features
y_income = df['income']

X_train_income, X_test_income, y_train_income, y_test_income = train_test_split(
    X_income, y_income, test_size=0.3, random_state=42
)

rf_income = RandomForestClassifier(
    random_state=42, n_estimators=100, min_samples_split=5,
    min_samples_leaf=2, max_depth=15, bootstrap=False
)
evaluate_model(rf_income, "Random Forest (Model 2)", X_train_income, X_test_income, y_train_income, y_test_income, "income", "rf_income_model.pkl")

print("All models have been trained and evaluated.")
print("==========================================================")
