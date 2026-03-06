from database import get_db_cursor

def check_saas():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT email FROM sys_users WHERE id = 1 OR username = 'superadmin'")
        rows = cursor.fetchall()
        print("SaaS Owner Candidate Emails:")
        for row in rows:
            print(row['email'])

if __name__ == "__main__":
    check_saas()
