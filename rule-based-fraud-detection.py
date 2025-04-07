import pandas as pd
import numpy as np
 
df = pd.read_csv("denormalized_transactions.csv")
# ----------------------------
# Smart Heuristics Definitions
# ----------------------------
 
# Faker-based blacklisted merchants and locations (randomized examples)
blacklisted_merchants = ['Vision Corp', 'Pinnacle Ltd', 'Omega LLC']
blacklisted_locations = ['Lakeview', 'Springfield', 'Newport']
 
# High amount threshold based on Faker range: upper 5% (since 95th percentile)
high_amount_threshold = df['amount'].quantile(0.95)  # ~₹4700 if Faker upper is ₹5000
 
# --- Rule 1: Low Closing Balance ---
df['flag_low_balance'] = df['closing_balance'] < 1000
 
# --- Rule 2: Blacklisted Merchant ---
df['flag_blacklisted_merchant'] = df['merchant'].isin(blacklisted_merchants)
 
# --- Rule 3: Blacklisted Location ---
df['flag_blacklisted_location'] = df['location'].isin(blacklisted_locations)
 
# --- Rule 4: High Amount ---
df['flag_high_amount'] = df['amount'] > high_amount_threshold
 
# --- Rule 5: Foreign + High-Risk Country ---
df['flag_high_risk_foreign'] = (df['is_foreign'] == True) & (df['is_high_risk_country'] == True)
 
# --- Rule 6: Credit Account Doing Withdrawals/Payments Over ₹3000 ---
df['flag_credit_withdrawal'] = (df['account_type'] == 'credit') & \
                                df['transaction_type'].isin(['withdrawal', 'payment']) & \
                                (df['amount'] > 3000)
 
# ----------------------------
# Compile reasons and output
# ----------------------------
 
flags = [
    'flag_low_balance',
    'flag_blacklisted_merchant',
    'flag_blacklisted_location',
    'flag_high_amount',
    'flag_high_risk_foreign',
    'flag_credit_withdrawal'
]
 
# Filter rows that triggered any rule
fraud_df = df[df[flags].any(axis=1)].copy()
 
# Compile reason column
def explain_reasons(row):
    reasons = []
    if row['flag_low_balance']:
        reasons.append("Closing balance < ₹1000")
    if row['flag_blacklisted_merchant']:
        reasons.append("Blacklisted merchant")
    if row['flag_blacklisted_location']:
        reasons.append("Blacklisted location")
    if row['flag_high_amount']:
        reasons.append("High transaction amount (>95th percentile)")
    if row['flag_high_risk_foreign']:
        reasons.append("Foreign + High-risk country")
    if row['flag_credit_withdrawal']:
        reasons.append("Unusual withdrawal on credit account")
    return "; ".join(reasons)
 
fraud_df["reason"] = fraud_df.apply(explain_reasons, axis=1)
 
# Drop the flag columns before saving
fraud_df.drop(columns=flags, inplace=True)
 
# Save output
fraud_df.to_csv("flagged_frauds.csv", index=False)
print(f"✅ {len(fraud_df)} suspicious transactions saved to 'flagged_frauds.csv'")