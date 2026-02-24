import psycopg2
from psycopg2 import sql

def setup_database():
    # Veritabanı bağlantı bilgilerini buraya girin
    connection_params = {
        "host": "localhost",
        "database": "your_db_name",
        "user": "your_username",
        "password": "your_password"
    }

    try:
        conn = psycopg2.connect(**connection_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Tabloyu Oluşturma
        create_table_query = """
        CREATE TABLE IF NOT EXISTS daily_official (
            symbol VARCHAR(20) NOT NULL,
            session_date DATE NOT NULL,
            official_open NUMERIC(18,6) NOT NULL,
            official_close NUMERIC(18,6) NOT NULL,
            source_open TEXT NOT NULL,
            source_close TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, session_date)
        );
        """
        cursor.execute(create_table_query)
        print("Tablo kontrol edildi/oluşturuldu.")

        # 2. session_date için Index Oluşturma
        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_daily_official_session_date 
        ON daily_official (session_date);
        """
        cursor.execute(create_index_query)
        print("Index kontrol edildi/oluşturuldu.")

        # 3. Trigger Fonksiyonu Oluşturma
        # (PostgreSQL'de trigger'dan önce fonksiyon tanımlanmalıdır)
        create_function_query = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        """
        cursor.execute(create_function_query)

        # 4. Trigger'ı Temizle ve Yeniden Oluştur
        # Önce varsa sil (Drop if exists), sonra oluştur
        drop_trigger_query = "DROP TRIGGER IF EXISTS set_updated_at ON daily_official;"
        cursor.execute(drop_trigger_query)

        create_trigger_query = """
        CREATE TRIGGER set_updated_at
        BEFORE UPDATE ON daily_official
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
        """
        cursor.execute(create_trigger_query)
        print("Trigger temizlendi ve yeniden oluşturuldu.")

        cursor.close()
        conn.close()
        print("\nİşlem başarıyla tamamlandı.")

    except Exception as e:
        print(f"Bir hata oluştu: {e}")

if __name__ == "__main__":
    setup_database()
