import os
import logging
import asyncio
import json
import uuid
import csv
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from enum import Enum
from decimal import Decimal
from aiohttp import web

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.types import InputFile, ContentType, InputMediaPhoto
from aiogram.utils.exceptions import BotBlocked, ChatNotFound
from aiogram.utils.markdown import escape_md
import aioschedule


# --- Код для поддержки работоспособности на Render ---
async def handle(request):
    return web.Response(text="Bot is alive")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Web server started on port {port}")
# ---------------------------------------------------

# ==================== КОНФИГУРАЦИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '0').split(',') if x.strip()]
DB_URL = os.getenv('DATABASE_URL')

# Инициализация бота
bot = Bot(token=TOKEN, parse_mode='HTML')
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ==================== БАЗА ДАННЫХ ====================
class Database:
    def __init__(self):
        self.pool = None
        self.stats_cache = {}
        self.cache_timeout = 300  # 5 минут
    
    async def connect(self):
        """Установка соединения с базой данных"""
        if self.pool:
            return
            
        try:
            # Получаем URL из переменной окружения
            database_url = os.environ.get("DATABASE_URL")
            
            if not database_url:
                logger.error("Переменная окружения DATABASE_URL не установлена!")
                return

            # Создаем пул соединений
            self.pool = await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=5,
                # ВАЖНО для Supabase: отключаем кэш стейтментов
                statement_cache_size=0, 
                command_timeout=60,
                server_settings={
                    "application_name": "tg_bot"
                }
            )
            logger.info("Соединение с базой данных Supabase установлено")
            
            # Сразу пробуем создать таблицы, если их нет
            await self.create_tables()
            
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных: {e}")
            self.pool = None
            # Если база не подключилась, лучше остановить запуск, 
            # чтобы не получать ошибки NoneType постоянно
            raise e
        
        
    async def create_tables(self):
        """Создание всех таблиц"""
        async with self.pool.acquire() as conn:
            # Пользователи
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    username VARCHAR(100),
                    full_name VARCHAR(200) NOT NULL,
                    phone VARCHAR(20),
                    email VARCHAR(100),
                    city VARCHAR(100),
                    event_date DATE,
                    referral_code VARCHAR(20) UNIQUE NOT NULL,
                    referrer_id INTEGER REFERENCES users(id),
                    balance INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    pending_earnings INTEGER DEFAULT 0,
                    total_orders INTEGER DEFAULT 0,
                    total_spent INTEGER DEFAULT 0,
                    is_vip BOOLEAN DEFAULT FALSE,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Настройки пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER REFERENCES users(id) PRIMARY KEY,
                    order_notifications BOOLEAN DEFAULT TRUE,
                    bonus_notifications BOOLEAN DEFAULT TRUE,
                    news_notifications BOOLEAN DEFAULT FALSE,
                    consultation_reminders BOOLEAN DEFAULT TRUE
                )
            ''')
            
            # Заказы
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    order_number VARCHAR(20) UNIQUE NOT NULL,
                    user_id INTEGER REFERENCES users(id) NOT NULL,
                    game_name VARCHAR(200),
                    occasion TEXT,
                    target_audience VARCHAR(50),
                    budget VARCHAR(50),
                    players_count VARCHAR(50),
                    emotions JSONB,
                    game_basis TEXT,
                    source VARCHAR(50),
                    play_frequency VARCHAR(50),
                    description TEXT,
                    telegram_username VARCHAR(100),
                    
                    -- Финансы
                    price INTEGER,
                    paid_amount INTEGER DEFAULT 0,
                    discount_percent INTEGER DEFAULT 0,
                    
                    -- Прогресс
                    current_stage INTEGER DEFAULT 1,
                    total_stages INTEGER DEFAULT 9,
                    progress_percent INTEGER DEFAULT 0,
                    
                    -- Менеджмент
                    manager_id INTEGER REFERENCES users(id),
                    deadline DATE,
                    status VARCHAR(20) DEFAULT 'new',
                    
                    -- Трекинг
                    started_at TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Этапы заказа
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS order_stages (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER REFERENCES orders(id) NOT NULL,
                    stage_number INTEGER NOT NULL,
                    stage_name VARCHAR(100) NOT NULL,
                    description TEXT,
                    start_date DATE,
                    end_date DATE,
                    completed BOOLEAN DEFAULT FALSE,
                    completed_at TIMESTAMP,
                    notes TEXT,
                    manager_comment TEXT
                )
            ''')
            
            # Статусы заказа
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS order_status_history (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER REFERENCES orders(id) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    changed_by INTEGER REFERENCES users(id),
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Консультации
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS consultations (
                    id SERIAL PRIMARY KEY,
                    consultation_number VARCHAR(20) UNIQUE NOT NULL,
                    user_id INTEGER REFERENCES users(id) NOT NULL,
                    consultation_date DATE NOT NULL,
                    consultation_time TIME NOT NULL,
                    duration INTEGER DEFAULT 45,
                    price INTEGER DEFAULT 450,
                    paid_amount INTEGER DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'pending',
                    payment_confirmed BOOLEAN DEFAULT FALSE,
                    receipt_sent BOOLEAN DEFAULT FALSE,
                    manager_id INTEGER REFERENCES users(id),
                    meeting_link TEXT,
                    notes TEXT,
                    feedback TEXT,
                    rating INTEGER,
                    conversion_to_order BOOLEAN DEFAULT FALSE,
                    reminder_sent BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Слоты консультаций
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS consultation_slots (
                    id SERIAL PRIMARY KEY,
                    slot_date DATE NOT NULL,
                    slot_time TIME NOT NULL,
                    is_available BOOLEAN DEFAULT TRUE,
                    booked_by INTEGER REFERENCES users(id),
                    created_by_admin INTEGER REFERENCES users(id) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(slot_date, slot_time)
                )
            ''')
            
            # Бонусы
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bonuses (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description TEXT NOT NULL,
                    detailed_description TEXT,
                    reward INTEGER NOT NULL,
                    conditions JSONB NOT NULL,
                    duration_days INTEGER NOT NULL,
                    max_activations INTEGER DEFAULT 1,
                    can_combine BOOLEAN DEFAULT FALSE,
                    requirements TEXT,
                    status VARCHAR(20) DEFAULT 'active',
                    icon VARCHAR(10),
                    position INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Активные бонусы пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_bonuses (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id) NOT NULL,
                    bonus_id INTEGER REFERENCES bonuses(id) NOT NULL,
                    progress INTEGER DEFAULT 0,
                    total_required INTEGER NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status VARCHAR(20) DEFAULT 'active',
                    proof_data TEXT,
                    completed_at TIMESTAMP,
                    reward_paid BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, bonus_id)
                )
            ''')
            
            # Выплаты
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS payouts (
                    id SERIAL PRIMARY KEY,
                    payout_number VARCHAR(20) UNIQUE NOT NULL,
                    user_id INTEGER REFERENCES users(id) NOT NULL,
                    amount INTEGER NOT NULL,
                    card_number VARCHAR(20) NOT NULL,
                    card_holder VARCHAR(100) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    processed_at TIMESTAMP,
                    processed_by INTEGER REFERENCES users(id),
                    rejection_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Чеки оплат
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS receipts (
                    id SERIAL PRIMARY KEY,
                    receipt_number VARCHAR(20) UNIQUE NOT NULL,
                    user_id INTEGER REFERENCES users(id) NOT NULL,
                    amount INTEGER NOT NULL,
                    payment_type VARCHAR(50) NOT NULL,
                    receipt_data TEXT,
                    order_id INTEGER REFERENCES orders(id),
                    consultation_id INTEGER REFERENCES consultations(id),
                    confirmed BOOLEAN DEFAULT FALSE,
                    confirmed_by INTEGER REFERENCES users(id),
                    confirmed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Портфолио
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS portfolio (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    description TEXT NOT NULL,
                    game_type VARCHAR(100),
                    client_name VARCHAR(100),
                    rating DECIMAL(3,2) DEFAULT 0,
                    reviews_count INTEGER DEFAULT 0,
                    photos JSONB,
                    views_count INTEGER DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'published',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Отзывы портфолио
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_reviews (
                    id SERIAL PRIMARY KEY,
                    portfolio_id INTEGER REFERENCES portfolio(id) NOT NULL,
                    client_name VARCHAR(100),
                    review_text TEXT NOT NULL,
                    rating INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Системные настройки
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS system_settings (
                    key VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Администраторы
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER REFERENCES users(id) PRIMARY KEY,
                    permissions JSONB DEFAULT '["all"]',
                    added_by INTEGER REFERENCES users(id),
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Уведомления
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    notification_type VARCHAR(50) NOT NULL,
                    user_id INTEGER REFERENCES users(id),
                    admin_only BOOLEAN DEFAULT FALSE,
                    data JSONB NOT NULL,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Рассылки
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS mailings (
                    id SERIAL PRIMARY KEY,
                    mailing_number VARCHAR(20) UNIQUE NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    message TEXT NOT NULL,
                    audience_type VARCHAR(50) NOT NULL,
                    filters JSONB,
                    total_recipients INTEGER DEFAULT 0,
                    sent_count INTEGER DEFAULT 0,
                    read_count INTEGER DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'draft',
                    sent_by INTEGER REFERENCES users(id),
                    sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # История действий
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    action_type VARCHAR(50) NOT NULL,
                    details JSONB,
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Индексы для производительности
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_referrer_id ON users(referrer_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_consultations_user_id ON consultations(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_consultations_date ON consultations(consultation_date)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_notifications_type ON notifications(notification_type)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_activity_log_user_id ON activity_log(user_id)')
            
            logger.info("Таблицы базы данных созданы")
    
    async def initialize_default_data(self):
        """Инициализация начальных данных"""
        async with self.pool.acquire() as conn:
            # Системные настройки
            default_settings = [
                ('min_payout', '2000', 'Минимальная сумма вывода'),
                ('referral_percentage', '10', 'Реферальный процент'),
                ('referral_bonus', '400', 'Фиксированный бонус за реферала'),
                ('consultation_price', '450', 'Стоимость консультации'),
                ('consultation_duration', '45', 'Длительность консультации (минуты)'),
                ('work_days', 'Понедельник-Пятница', 'Рабочие дни'),
                ('work_hours', '10:00-20:00', 'Рабочие часы'),
                ('break_time', '13:00-14:00', 'Перерыв'),
                ('phone', '+7 (925) 101-56-63', 'Контактный телефон'),
                ('email', 'timporsh97@icloud.com', 'Email'),
                ('manager_username', '@bgh_997', 'Менеджер в Telegram'),
                ('city', 'Москва', 'Город'),
                ('bank_name', 'Тинькофф', 'Название банка'),
                ('card_number', '2200 **** **** 5678', 'Номер карты'),
                ('card_holder', 'Тимофей', 'Имя получателя'),
                ('payment_timeout', '3', 'Таймаут оплаты (часы)'),
                ('order_stages', '9', 'Количество этапов заказа'),
                ('incomplete_order_hours', '36', 'Часов до напоминания о незавершенной заявке'),
                ('reminder_hours', '24', 'За сколько часов напоминать о консультации'),
                ('daily_report_time', '09:00', 'Время ежедневного отчета'),
                ('system_commission', '0', 'Комиссия системы (%)')
            ]
            
            for key, value, description in default_settings:
                await conn.execute('''
                    INSERT INTO system_settings (key, value, description)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                ''', key, value, description)
            
            # Бонусы
            bonuses_data = [
                (
                    'Летописец в соцсетях',
                    'Стабильно рассказывать о нас в своих социальных сетях.',
                    '🎯 УСЛОВИЯ ВЫПОЛНЕНИЯ:\n\n1. Публикации: 1 пост в неделю (4 поста за месяц)\n2. Качество: Уникальный контент (не репост) с упоминанием бота\n3. Проверка: В конце месяца отправляете ссылки на все 4 поста\n4. Срок выполнения: 30 календарных дней с активации\n\n📊 МИНИМАЛЬНАЯ АУДИТОРИЯ:\n• Instagram – от 1 000 подписчиков\n• YouTube – от 5 000 подписчиков\n• TikTok – от 10 000 подписчиков\n• Telegram-канал – от 1 000 подписчиков\n\n💎 ОСОБЫЕ УСЛОВИЯ:\n• 10 000+ подписчиков – награда обсуждается с менеджером @bgh_997\n• «Ваши искренние слова — лучшая рекомендация. Пусть ваша лента станет источником вдохновения для новых создателей.»',
                    300,
                    json.dumps({
                        "posts_per_week": 1,
                        "weeks_required": 4,
                        "min_followers": {
                            "instagram": 1000,
                            "youtube": 5000,
                            "tiktok": 10000,
                            "telegram": 1000
                        },
                        "report_to": "manager",
                        "reward_increase": 200
                    }),
                    30,
                    1,
                    True,
                    'Минимальная аудитория в соцсетях',
                    '📱',
                    1
                ),
                (
                    'Быстрый старт',
                    'Проявить максимальную активность в первые дни.',
                    '🎯 УСЛОВИЯ ВЫПОЛНЕНИЯ:\n\n1. Период: 7 дней с момента активации\n2. Задача: Привести 3 клиентов за 7 дней\n3. Критерии клиентов:\n   • Новые клиенты (не заказывали ранее)\n   • Совершают первую оплату (консультации или аванс за заказ)\n   • Оплата должна поступить в течение 7 дней\n4. Подтверждение: Менеджер проверяет каждую оплату\n5. Награда: +300₽ дополнительно к другим выплатам\n\n«Докажите свою силу влияния сразу и получите специальный приз за скорость!»',
                    300,
                    json.dumps({
                        "clients_required": 3,
                        "days_limit": 7,
                        "must_be_new": True,
                        "payment_required": True,
                        "additional_to_other": True
                    }),
                    7,
                    1,
                    True,
                    'Привести 3 клиентов за 7 дней',
                    '⚡',
                    2
                ),
                (
                    'Охотник за сокровищами',
                    'Привести первого крупного клиента.',
                    '🎯 УСЛОВИЯ ВЫПОЛНЕНИЯ:\n\n1. Период: 30 дней с момента активации\n2. Задача: Привлечь клиента с первым заказом от 10 000₽\n3. Критерии:\n   • Клиент новый (не заказывал ранее)\n   • Учитывается первый платёж (аванс) от клиента\n   • Минимальная сумма первого платежа: 10 000₽\n4. Проверка: Менеджер подтверждает поступление оплаты\n5. Выплата: Единоразовая выплата 1000₽ после получения аванса\n\n«Найдите того, чья легенда будет эпической. И ваша награда будет королевской.»',
                    1000,
                    json.dumps({
                        "min_order_amount": 10000,
                        "days_limit": 30,
                        "must_be_new": True,
                        "first_payment_only": True
                    }),
                    30,
                    1,
                    True,
                    'Клиент с бюджетом от 10 000₽',
                    '🏴‍☠️',
                    3
                ),
                (
                    'Покровитель',
                    'Приводить активных партнёров.',
                    '🎯 УСЛОВИЯ ВЫПОЛНЕНИЯ:\n\n1. Условия для реферала:\n   • Зарегистрирован по вашей ссылке\n   • Совершил хотя бы одну оплату\n2. Процент: 3% от суммы каждого выполненного заказа реферала\n3. Что считается заказом:\n   • Любая оплата от клиента реферала\n   • Консультации, заказы, дополнительные услуги\n4. Выплаты: Ежемесячно, 5-го числа за предыдущий месяц\n5. Срок действия: Бессрочно, пока реферал активен\n\n«Создайте свою сеть создателей игр. Ваша мудрость и связи будут приносить плоды снова и снова.»',
                    0,
                    json.dumps({
                        "percentage": 3,
                        "from_orders": True,
                        "from_consultations": True,
                        "payout_day": 5,
                        "lifetime": True
                    }),
                    0,
                    0,
                    True,
                    '3% от заказов реферала',
                    '💰',
                    4
                )
            ]
            
            for bonus in bonuses_data:
                await conn.execute('''
                    INSERT INTO bonuses (
                        name, description, detailed_description, reward, conditions,
                        duration_days, max_activations, can_combine, requirements, icon, position
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT DO NOTHING
                ''', *bonus)
            
            logger.info("Начальные данные инициализированы")
    
    # ==================== ПОЛЬЗОВАТЕЛИ ====================
    
    async def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получить пользователя по telegram_id"""
        async with self.pool.acquire() as conn:
            user = await conn.fetchrow('''
                SELECT u.*, 
                       us.order_notifications, us.bonus_notifications, 
                       us.news_notifications, us.consultation_reminders,
                       (SELECT COUNT(*) FROM users WHERE referrer_id = u.id) as referrals_count,
                       (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as total_orders_count,
                       (SELECT COALESCE(SUM(price), 0) FROM orders WHERE user_id = u.id AND status = 'completed') as total_spent_amount,
                       (SELECT COUNT(*) FROM consultations WHERE user_id = u.id) as total_consultations_count,
                       EXISTS(SELECT 1 FROM admins WHERE user_id = u.id) as is_admin
                FROM users u
                LEFT JOIN user_settings us ON u.id = us.user_id
                WHERE u.telegram_id = $1
            ''', telegram_id)
            
            if user:
                return dict(user)
            return None
    
    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Получить пользователя по ID"""
        async with self.pool.acquire() as conn:
            user = await conn.fetchrow('SELECT * FROM users WHERE id = $1', user_id)
            return dict(user) if user else None
    
    async def create_user(self, telegram_id: int, username: str, full_name: str, referrer_code: str = None) -> Dict:
        """Создать нового пользователя"""
        async with self.pool.acquire() as conn:
            # Генерация реферального кода
            referral_code = f"REF{telegram_id}{uuid.uuid4().hex[:6].upper()}"
            
            # Поиск реферера
            referrer_id = None
            if referrer_code and referrer_code != 'start':
                referrer_code_clean = referrer_code.replace('ref_', '')
                referrer = await conn.fetchrow(
                    'SELECT id FROM users WHERE referral_code = $1',
                    referrer_code_clean
                )
                if referrer:
                    referrer_id = referrer['id']
            
            # Создание пользователя
            user = await conn.fetchrow('''
                INSERT INTO users (telegram_id, username, full_name, referral_code, referrer_id)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            ''', telegram_id, username, full_name, referral_code, referrer_id)
            
            # Создание настроек по умолчанию
            await conn.execute('''
                INSERT INTO user_settings (user_id) VALUES ($1)
            ''', user['id'])
            
            # Логирование
            await self.log_activity(user['id'], 'user_registration', {
                'referrer_id': referrer_id,
                'referral_code': referral_code
            })
            
            # Уведомление рефереру
            if referrer_id:
                await self.create_notification(
                    'new_referral',
                    referrer_id,
                    {
                        'new_user_id': user['id'],
                        'new_user_name': full_name,
                        'new_user_username': username
                    }
                )
            
            return dict(user)
    
    async def update_user_profile(self, telegram_id: int, **kwargs) -> bool:
        """Обновить профиль пользователя"""
        async with self.pool.acquire() as conn:
            user = await self.get_user(telegram_id)
            if not user:
                return False
            
            valid_fields = ['full_name', 'phone', 'email', 'city', 'event_date']
            update_data = {k: v for k, v in kwargs.items() if k in valid_fields and v is not None}
            
            if update_data:
                set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(update_data.keys())])
                values = list(update_data.values())
                
                await conn.execute(f'''
                    UPDATE users 
                    SET {set_clause}, updated_at = CURRENT_TIMESTAMP
                    WHERE telegram_id = $1
                ''', telegram_id, *values)
                
                await self.log_activity(user['id'], 'profile_update', {'fields': list(update_data.keys())})
                return True
            
            return False
    
    async def update_user_settings(self, user_id: int, **kwargs) -> bool:
        """Обновить настройки пользователя"""
        async with self.pool.acquire() as conn:
            valid_fields = ['order_notifications', 'bonus_notifications', 'news_notifications', 'consultation_reminders']
            update_data = {k: v for k, v in kwargs.items() if k in valid_fields and v is not None}
            
            if update_data:
                # Проверяем существование записи
                exists = await conn.fetchval('SELECT 1 FROM user_settings WHERE user_id = $1', user_id)
                
                if exists:
                    set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(update_data.keys())])
                    values = list(update_data.values())
                    
                    await conn.execute(f'''
                        UPDATE user_settings 
                        SET {set_clause}
                        WHERE user_id = $1
                    ''', user_id, *values)
                else:
                    columns = ['user_id'] + list(update_data.keys())
                    placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
                    values = [user_id] + list(update_data.values())
                    
                    await conn.execute(f'''
                        INSERT INTO user_settings ({', '.join(columns)})
                        VALUES ({placeholders})
                    ''', *values)
                
                await self.log_activity(user_id, 'settings_update', {'fields': list(update_data.keys())})
                return True
            
            return False
    
    async def update_user_balance(self, user_id: int, amount: int, reason: str, details: Dict = None) -> bool:
        """Обновить баланс пользователя"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем баланс
                await conn.execute('''
                    UPDATE users 
                    SET balance = balance + $1,
                        total_earned = total_earned + CASE WHEN $1 > 0 THEN $1 ELSE 0 END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2
                ''', amount, user_id)
                
                # Логируем операцию
                await self.log_activity(user_id, 'balance_update', {
                    'amount': amount,
                    'reason': reason,
                    'details': details or {}
                })
                
                return True
    
    # ==================== ЗАКАЗЫ ====================
    
    async def create_order(self, user_id: int, data: Dict) -> Dict:
        """Создать новый заказ"""
        async with self.pool.acquire() as conn:
            # Генерация номера заказа
            order_number = f"SG{datetime.now().strftime('%y%m%d')}{uuid.uuid4().hex[:5].upper()}"
            
            # Создание заказа
            order = await conn.fetchrow('''
                INSERT INTO orders (
                    order_number, user_id, game_name, occasion, target_audience,
                    budget, players_count, emotions, game_basis, source,
                    play_frequency, description, telegram_username,
                    started_at, last_activity
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING *
            ''', order_number, user_id, 
                data.get('game_name'), data.get('occasion'), data.get('target_audience'),
                data.get('budget'), data.get('players_count'), json.dumps(data.get('emotions', [])),
                data.get('game_basis'), data.get('source'), data.get('play_frequency'),
                data.get('description'), data.get('telegram_username')
            )
            
            # Создание этапов заказа
            stages = [
                (1, 'Анализ требований', 'Изучение требований клиента, согласование идеи и бюджета'),
                (2, 'Разработка концепции', 'Создание концепции игры, продумывание механики и сюжета'),
                (3, 'Дизайн игрового поля', 'Разработка дизайна игрового поля и визуального стиля'),
                (4, 'Создание прототипа', 'Создание физического прототипа для тестирования'),
                (5, 'Балансировка игрового процесса', 'Тестирование и балансировка игровых механик'),
                (6, 'Дизайн компонентов', 'Дизайн карточек, фигурок, инструкции и упаковки'),
                (7, 'Тестирование', 'Тестирование игры с фокус-группой, сбор отзывов'),
                (8, 'Финальные правки', 'Внесение финальных правок по результатам тестирования'),
                (9, 'Подготовка к отправке', 'Подготовка финальной версии, упаковка и логистика')
            ]
            
            for stage_num, stage_name, stage_desc in stages:
                await conn.execute('''
                    INSERT INTO order_stages (order_id, stage_number, stage_name, description)
                    VALUES ($1, $2, $3, $4)
                ''', order['id'], stage_num, stage_name, stage_desc)
            
            # Обновление счетчика заказов пользователя
            await conn.execute('''
                UPDATE users SET total_orders = total_orders + 1 WHERE id = $1
            ''', user_id)
            
            # Уведомление админам
            user = await self.get_user_by_id(user_id)
            await self.create_notification(
                'new_order',
                None,  # Для всех админов
                {
                    'order_id': order['id'],
                    'order_number': order_number,
                    'user_id': user_id,
                    'user_name': user['full_name'],
                    'user_phone': user.get('phone'),
                    'user_telegram': user.get('username'),
                    'game_name': data.get('game_name'),
                    'budget': data.get('budget')
                },
                admin_only=True
            )
            
            # Логирование
            await self.log_activity(user_id, 'order_created', {'order_id': order['id'], 'order_number': order_number})
            
            return dict(order)
    
    async def get_user_orders(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Получить заказы пользователя"""
        async with self.pool.acquire() as conn:
            orders = await conn.fetch('''
                SELECT o.*, 
                       (SELECT COUNT(*) FROM order_stages WHERE order_id = o.id AND completed = TRUE) as completed_stages
                FROM orders o
                WHERE o.user_id = $1
                ORDER BY o.created_at DESC
                LIMIT $2
            ''', user_id, limit)
            
            return [dict(order) for order in orders]
    
    async def get_order(self, order_id: int) -> Optional[Dict]:
        """Получить заказ по ID"""
        async with self.pool.acquire() as conn:
            order = await conn.fetchrow('''
                SELECT o.*, u.full_name, u.phone, u.email, u.city,
                       (SELECT COUNT(*) FROM order_stages WHERE order_id = o.id AND completed = TRUE) as completed_stages
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.id = $1
            ''', order_id)
            
            return dict(order) if order else None
    
    async def get_order_by_number(self, order_number: str) -> Optional[Dict]:
        """Получить заказ по номеру"""
        async with self.pool.acquire() as conn:
            order = await conn.fetchrow('SELECT * FROM orders WHERE order_number = $1', order_number)
            return dict(order) if order else None
    
    async def get_order_tracker(self, order_id: int) -> Dict:
        """Получить трекер заказа с этапами"""
        async with self.pool.acquire() as conn:
            # Получаем заказ
            order = await conn.fetchrow('''
                SELECT o.*, u.full_name as user_name, u.phone, u.email, u.city,
                       (SELECT COUNT(*) FROM order_stages WHERE order_id = o.id AND completed = TRUE) as completed_stages
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.id = $1
            ''', order_id)
            
            if not order:
                return {}
            
            # Получаем этапы
            stages = await conn.fetch('''
                SELECT * FROM order_stages 
                WHERE order_id = $1 
                ORDER BY stage_number
            ''', order_id)
            
            # Получаем последний комментарий менеджера
            last_comment = await conn.fetchval('''
                SELECT manager_comment FROM order_stages 
                WHERE order_id = $1 AND manager_comment IS NOT NULL 
                ORDER BY stage_number DESC LIMIT 1
            ''', order_id)
            
            # Рассчитываем процент выполнения
            completed_stages = order['completed_stages'] or 0
            total_stages = order['total_stages'] or 9
            
            return {
                'order': dict(order),
                'stages': [dict(stage) for stage in stages],
                'last_manager_comment': last_comment,
                'progress_percent': int((completed_stages / total_stages) * 100) if total_stages > 0 else 0
            }
    
    async def update_order_stage(self, order_id: int, stage_number: int, completed: bool = True, manager_comment: str = None) -> bool:
        """Обновить этап заказа"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем этап
                await conn.execute('''
                    UPDATE order_stages 
                    SET completed = $1, 
                        completed_at = CASE WHEN $1 THEN CURRENT_TIMESTAMP ELSE NULL END,
                        manager_comment = COALESCE($3, manager_comment)
                    WHERE order_id = $2 AND stage_number = $4
                ''', completed, order_id, manager_comment, stage_number)
                
                # Обновляем прогресс заказа
                completed_stages = await conn.fetchval('''
                    SELECT COUNT(*) FROM order_stages 
                    WHERE order_id = $1 AND completed = TRUE
                ''', order_id)
                
                total_stages = await conn.fetchval('''
                    SELECT COUNT(*) FROM order_stages WHERE order_id = $1
                ''', order_id)
                
                progress_percent = int((completed_stages / total_stages) * 100) if total_stages > 0 else 0
                
                await conn.execute('''
                    UPDATE orders 
                    SET current_stage = $1,
                        progress_percent = $2,
                        last_activity = CURRENT_TIMESTAMP
                    WHERE id = $3
                ''', min(completed_stages + 1, total_stages), progress_percent, order_id)
                
                # Уведомление пользователю, если включены уведомления
                if completed:
                    order = await self.get_order(order_id)
                    if order:
                        user_settings = await conn.fetchrow('''
                            SELECT order_notifications FROM user_settings WHERE user_id = $1
                        ''', order['user_id'])
                        
                        if user_settings and user_settings['order_notifications']:
                            # Получаем название этапа
                            stage_info = await conn.fetchrow('''
                                SELECT stage_name FROM order_stages 
                                WHERE order_id = $1 AND stage_number = $2
                            ''', order_id, stage_number)
                            
                            stage_name = stage_info['stage_name'] if stage_info else 'Этап'
                            
                            await self.create_notification(
                                'order_stage_completed',
                                order['user_id'],
                                {
                                    'order_id': order_id,
                                    'order_number': order['order_number'],
                                    'stage_number': stage_number,
                                    'stage_name': stage_name,
                                    'total_stages': total_stages,
                                    'completed_stages': completed_stages
                                }
                            )
                
                return True
    
    async def update_order_price(self, order_id: int, price: int) -> bool:
        """Обновить цену заказа"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE orders SET price = $1, last_activity = CURRENT_TIMESTAMP WHERE id = $2
            ''', price, order_id)
            
            order = await self.get_order(order_id)
            if order:
                await self.log_activity(None, 'order_price_updated', {
                    'order_id': order_id,
                    'order_number': order['order_number'],
                    'price': price,
                    'user_id': order['user_id']
                })
            
            return True
    
    async def update_order_status(self, order_id: int, status: str, changed_by: int = None, notes: str = None) -> bool:
        """Обновить статус заказа"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем статус заказа
                await conn.execute('''
                    UPDATE orders 
                    SET status = $1, last_activity = CURRENT_TIMESTAMP,
                        completed_at = CASE WHEN $1 = 'completed' THEN CURRENT_TIMESTAMP ELSE completed_at END
                    WHERE id = $2
                ''', status, order_id)
                
                # Записываем в историю
                await conn.execute('''
                    INSERT INTO order_status_history (order_id, status, changed_by, notes)
                    VALUES ($1, $2, $3, $4)
                ''', order_id, status, changed_by, notes)
                
                # Логирование
                if changed_by:
                    await self.log_activity(changed_by, 'order_status_changed', {
                        'order_id': order_id,
                        'status': status,
                        'notes': notes
                    })
                
                return True
    
    # ==================== КОНСУЛЬТАЦИИ ====================
    
    async def add_consultation_slot(self, admin_id: int, slot_date: str, slot_time: str) -> bool:
        """Добавить слот консультации"""
        async with self.pool.acquire() as conn:
            try:
                date_obj = datetime.strptime(slot_date, '%Y-%m-%d').date()
                time_obj = datetime.strptime(slot_time, '%H:%M').time()
                
                await conn.execute('''
                    INSERT INTO consultation_slots (slot_date, slot_time, created_by_admin)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (slot_date, slot_time) DO NOTHING
                ''', date_obj, time_obj, admin_id)
                
                await self.log_activity(admin_id, 'consultation_slot_added', {
                    'slot_date': slot_date,
                    'slot_time': slot_time
                })
                
                return True
            except Exception as e:
                logger.error(f"Ошибка добавления слота: {e}")
                return False
    
    async def get_available_slots(self, date: datetime = None) -> List[Dict]:
        """Получить доступные слоты"""
        async with self.pool.acquire() as conn:
            query = '''
                SELECT cs.*, u.username as admin_username
                FROM consultation_slots cs
                LEFT JOIN users u ON cs.created_by_admin = u.id
                WHERE cs.is_available = TRUE
            '''
            params = []
            
            if date:
                query += ' AND cs.slot_date = $1'
                params.append(date.date())
            
            query += ' ORDER BY cs.slot_date, cs.slot_time'
            
            slots = await conn.fetch(query, *params)
            return [dict(slot) for slot in slots]
    
    async def get_slots_by_date(self, date_str: str) -> List[Dict]:
        """Получить слоты по дате"""
        async with self.pool.acquire() as conn:
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                slots = await conn.fetch('''
                    SELECT * FROM consultation_slots 
                    WHERE slot_date = $1 AND is_available = TRUE
                    ORDER BY slot_time
                ''', date_obj)
                
                return [dict(slot) for slot in slots]
            except:
                return []
    
    async def book_consultation(self, user_id: int, slot_id: int) -> Optional[Dict]:
        """Забронировать консультацию"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Бронируем слот
                slot = await conn.fetchrow('''
                    UPDATE consultation_slots 
                    SET is_available = FALSE, booked_by = $1
                    WHERE id = $2 AND is_available = TRUE
                    RETURNING *
                ''', user_id, slot_id)
                
                if not slot:
                    return None
                
                # Создаем запись о консультации
                consultation_number = f"CONS{datetime.now().strftime('%y%m%d')}{uuid.uuid4().hex[:4].upper()}"
                
                # Получаем цену консультации из настроек
                consultation_price = await conn.fetchval(
                    "SELECT value::integer FROM system_settings WHERE key = 'consultation_price'"
                ) or 450
                
                consultation = await conn.fetchrow('''
                    INSERT INTO consultations (
                        consultation_number, user_id, consultation_date, 
                        consultation_time, price
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING *
                ''', consultation_number, user_id, slot['slot_date'], slot['slot_time'], consultation_price)
                
                # Уведомление админам
                user = await self.get_user_by_id(user_id)
                await self.create_notification(
                    'new_consultation',
                    None,
                    {
                        'consultation_id': consultation['id'],
                        'consultation_number': consultation_number,
                        'user_id': user_id,
                        'user_name': user['full_name'],
                        'user_phone': user.get('phone'),
                        'user_username': user.get('username'),
                        'date': slot['slot_date'].strftime('%d.%m.%Y'),
                        'time': slot['slot_time'].strftime('%H:%M'),
                        'price': consultation_price
                    },
                    admin_only=True
                )
                
                # Логирование
                await self.log_activity(user_id, 'consultation_booked', {
                    'consultation_id': consultation['id'],
                    'slot_id': slot_id,
                    'date': slot['slot_date'].strftime('%Y-%m-%d'),
                    'time': slot['slot_time'].strftime('%H:%M')
                })
                
                return dict(consultation)
    
    async def get_user_consultations(self, user_id: int) -> List[Dict]:
        """Получить консультации пользователя"""
        async with self.pool.acquire() as conn:
            consultations = await conn.fetch('''
                SELECT * FROM consultations 
                WHERE user_id = $1
                ORDER BY consultation_date DESC, consultation_time DESC
            ''', user_id)
            
            return [dict(consultation) for consultation in consultations]
    
    async def confirm_consultation_payment(self, consultation_id: int, admin_id: int) -> bool:
        """Подтвердить оплату консультации"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                consultation = await conn.fetchrow('''
                    UPDATE consultations 
                    SET payment_confirmed = TRUE,
                        status = 'confirmed',
                        manager_id = $1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2 AND payment_confirmed = FALSE
                    RETURNING *
                ''', admin_id, consultation_id)
                
                if not consultation:
                    return False
                
                # Уведомление пользователю
                manager_username = await self.get_admin_username(admin_id) or 'менеджер'
                await self.create_notification(
                    'consultation_confirmed',
                    consultation['user_id'],
                    {
                        'consultation_id': consultation_id,
                        'consultation_number': consultation['consultation_number'],
                        'date': consultation['consultation_date'].strftime('%d.%m.%Y'),
                        'time': consultation['consultation_time'].strftime('%H:%M'),
                        'manager_username': manager_username
                    }
                )
                
                # Логирование
                await self.log_activity(admin_id, 'consultation_payment_confirmed', {
                    'consultation_id': consultation_id,
                    'user_id': consultation['user_id']
                })
                
                return True
    
    async def get_consultation(self, consultation_id: int) -> Optional[Dict]:
        """Получить консультацию по ID"""
        async with self.pool.acquire() as conn:
            consultation = await conn.fetchrow('SELECT * FROM consultations WHERE id = $1', consultation_id)
            return dict(consultation) if consultation else None
    
    async def get_todays_consultations(self) -> List[Dict]:
        """Получить сегодняшние консультации"""
        async with self.pool.acquire() as conn:
            today = datetime.now().date()
            consultations = await conn.fetch('''
                SELECT c.*, u.full_name, u.username, u.phone
                FROM consultations c
                JOIN users u ON c.user_id = u.id
                WHERE c.consultation_date = $1
                ORDER BY c.consultation_time
            ''', today)
            
            return [dict(consultation) for consultation in consultations]
    
    # ==================== БОНУСЫ ====================
    
    async def get_bonuses(self, active_only: bool = True) -> List[Dict]:
        """Получить список бонусов"""
        async with self.pool.acquire() as conn:
            query = 'SELECT * FROM bonuses'
            if active_only:
                query += " WHERE status = 'active'"
            query += ' ORDER BY position'
            
            bonuses = await conn.fetch(query)
            return [dict(bonus) for bonus in bonuses]
    
    async def get_bonus(self, bonus_id: int) -> Optional[Dict]:
        """Получить бонус по ID"""
        async with self.pool.acquire() as conn:
            bonus = await conn.fetchrow('SELECT * FROM bonuses WHERE id = $1', bonus_id)
            return dict(bonus) if bonus else None
    
    async def activate_bonus(self, user_id: int, bonus_id: int) -> Optional[Dict]:
        """Активировать бонус для пользователя"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Проверяем лимит активных бонусов
                active_count = await conn.fetchval('''
                    SELECT COUNT(*) FROM user_bonuses 
                    WHERE user_id = $1 AND status = 'active'
                ''', user_id)
                
                if active_count >= 2:
                    return None
                
                # Проверяем, не активирован ли уже этот бонус
                existing = await conn.fetchval('''
                    SELECT 1 FROM user_bonuses 
                    WHERE user_id = $1 AND bonus_id = $2 AND status IN ('active', 'pending_review')
                ''', user_id, bonus_id)
                
                if existing:
                    return None
                
                # Получаем информацию о бонусе
                bonus = await self.get_bonus(bonus_id)
                if not bonus:
                    return None
                
                # Определяем total_required на основе типа бонуса
                conditions = json.loads(bonus['conditions'])
                total_required = 0
                
                if bonus_id == 1:  # Летописец в соцсетях
                    total_required = conditions.get('weeks_required', 4)
                elif bonus_id == 2:  # Быстрый старт
                    total_required = conditions.get('clients_required', 3)
                elif bonus_id == 3:  # Охотник за сокровищами
                    total_required = 1  # Нужен 1 крупный клиент
                else:
                    total_required = 1
                
                # Активируем бонус
                user_bonus = await conn.fetchrow('''
                    INSERT INTO user_bonuses (
                        user_id, bonus_id, start_date, end_date, total_required, status
                    ) VALUES ($1, $2, CURRENT_DATE, CURRENT_DATE + $3, $4, 'active')
                    RETURNING *
                ''', user_id, bonus_id, bonus['duration_days'], total_required)
                
                # Логирование
                await self.log_activity(user_id, 'bonus_activated', {
                    'bonus_id': bonus_id,
                    'bonus_name': bonus['name'],
                    'duration_days': bonus['duration_days']
                })
                
                return dict(user_bonus)
    
    async def get_user_bonuses(self, user_id: int) -> List[Dict]:
        """Получить бонусы пользователя"""
        async with self.pool.acquire() as conn:
            bonuses = await conn.fetch('''
                SELECT ub.*, b.name, b.description, b.reward, b.icon, b.detailed_description
                FROM user_bonuses ub
                JOIN bonuses b ON ub.bonus_id = b.id
                WHERE ub.user_id = $1
                ORDER BY ub.status, ub.end_date
            ''', user_id)
            
            return [dict(bonus) for bonus in bonuses]
    
    async def update_bonus_progress(self, user_bonus_id: int, progress: int) -> bool:
        """Обновить прогресс бонуса"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE user_bonuses 
                SET progress = $1
                WHERE id = $2
            ''', progress, user_bonus_id)
            
            return True
    
    async def complete_bonus(self, user_bonus_id: int, proof_data: str = None) -> Optional[Dict]:
        """Завершить бонус (отправить на проверку)"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                user_bonus = await conn.fetchrow('''
                    UPDATE user_bonuses 
                    SET status = 'pending_review', 
                        proof_data = $1,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE id = $2 AND status = 'active'
                    RETURNING *
                ''', proof_data, user_bonus_id)
                
                if not user_bonus:
                    return None
                
                # Получаем информацию о бонусе
                bonus = await self.get_bonus(user_bonus['bonus_id'])
                
                # Уведомление админам
                await self.create_notification(
                    'bonus_completion',
                    None,
                    {
                        'user_bonus_id': user_bonus_id,
                        'user_id': user_bonus['user_id'],
                        'bonus_id': user_bonus['bonus_id'],
                        'bonus_name': bonus['name'] if bonus else 'Бонус',
                        'proof_data': proof_data,
                        'progress': user_bonus['progress'],
                        'total_required': user_bonus['total_required']
                    },
                    admin_only=True
                )
                
                # Логирование
                await self.log_activity(user_bonus['user_id'], 'bonus_completed', {
                    'user_bonus_id': user_bonus_id,
                    'bonus_id': user_bonus['bonus_id']
                })
                
                return dict(user_bonus)
    
    async def approve_bonus(self, user_bonus_id: int, admin_id: int) -> bool:
        """Одобрить выполнение бонуса"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                user_bonus = await conn.fetchrow('''
                    SELECT ub.*, b.reward, u.balance
                    FROM user_bonuses ub
                    JOIN bonuses b ON ub.bonus_id = b.id
                    JOIN users u ON ub.user_id = u.id
                    WHERE ub.id = $1 AND ub.status = 'pending_review'
                ''', user_bonus_id)
                
                if not user_bonus:
                    return False
                
                # Обновляем статус бонуса
                await conn.execute('''
                    UPDATE user_bonuses 
                    SET status = 'completed', reward_paid = TRUE
                    WHERE id = $1
                ''', user_bonus_id)
                
                # Начисляем награду
                if user_bonus['reward'] > 0:
                    await self.update_user_balance(
                        user_bonus['user_id'],
                        user_bonus['reward'],
                        'bonus_reward',
                        {'bonus_id': user_bonus['bonus_id'], 'user_bonus_id': user_bonus_id}
                    )
                
                # Получаем название бонуса
                bonus = await self.get_bonus(user_bonus['bonus_id'])
                bonus_name = bonus['name'] if bonus else 'Бонус'
                
                # Уведомление пользователю
                await self.create_notification(
                    'bonus_approved',
                    user_bonus['user_id'],
                    {
                        'user_bonus_id': user_bonus_id,
                        'bonus_name': bonus_name,
                        'reward': user_bonus['reward']
                    }
                )
                
                # Логирование
                await self.log_activity(admin_id, 'bonus_approved', {
                    'user_bonus_id': user_bonus_id,
                    'user_id': user_bonus['user_id'],
                    'reward': user_bonus['reward']
                })
                
                return True
    
    async def reject_bonus(self, user_bonus_id: int, admin_id: int, reason: str) -> bool:
        """Отклонить выполнение бонуса"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE user_bonuses 
                SET status = 'rejected'
                WHERE id = $1
            ''', user_bonus_id)
            
            # Получаем информацию для уведомления
            user_bonus = await conn.fetchrow('''
                SELECT ub.*, u.telegram_id 
                FROM user_bonuses ub
                JOIN users u ON ub.user_id = u.id
                WHERE ub.id = $1
            ''', user_bonus_id)
            
            if user_bonus:
                # Уведомление пользователю
                await self.create_notification(
                    'bonus_rejected',
                    user_bonus['user_id'],
                    {
                        'user_bonus_id': user_bonus_id,
                        'reason': reason
                    }
                )
            
            # Логирование
            await self.log_activity(admin_id, 'bonus_rejected', {
                'user_bonus_id': user_bonus_id,
                'reason': reason
            })
            
            return True
    
    # ==================== ВЫПЛАТЫ ====================
    
    async def create_payout_request(self, user_id: int, amount: int, card_number: str, card_holder: str) -> Optional[Dict]:
        """Создать заявку на вывод"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Проверяем баланс
                user = await conn.fetchrow('SELECT balance FROM users WHERE id = $1', user_id)
                if user['balance'] < amount:
                    return None
                
                # Получаем минимальную сумму выплаты
                min_payout = await conn.fetchval(
                    "SELECT value::integer FROM system_settings WHERE key = 'min_payout'"
                ) or 2000
                
                if amount < min_payout:
                    return None
                
                # Генерируем номер заявки
                payout_number = f"PAY{datetime.now().strftime('%y%m%d')}{uuid.uuid4().hex[:4].upper()}"
                
                # Создаем заявку
                payout = await conn.fetchrow('''
                    INSERT INTO payouts (payout_number, user_id, amount, card_number, card_holder)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING *
                ''', payout_number, user_id, amount, card_number, card_holder)
                
                # Резервируем сумму на балансе
                await conn.execute('''
                    UPDATE users 
                    SET balance = balance - $1,
                        pending_earnings = pending_earnings + $1
                    WHERE id = $2
                ''', amount, user_id)
                
                # Уведомление админам
                card_last_four = card_number[-4:] if len(card_number) >= 4 else card_number
                await self.create_notification(
                    'new_payout',
                    None,
                    {
                        'payout_id': payout['id'],
                        'payout_number': payout_number,
                        'user_id': user_id,
                        'amount': amount,
                        'card_last_four': card_last_four
                    },
                    admin_only=True
                )
                
                # Логирование
                await self.log_activity(user_id, 'payout_requested', {
                    'payout_id': payout['id'],
                    'amount': amount,
                    'card_last_four': card_last_four
                })
                
                return dict(payout)
    
    async def get_payout_requests(self, status: str = None) -> List[Dict]:
        """Получить заявки на вывод"""
        async with self.pool.acquire() as conn:
            query = '''
                SELECT p.*, u.full_name, u.username, u.telegram_id
                FROM payouts p
                JOIN users u ON p.user_id = u.id
            '''
            params = []
            
            if status:
                query += ' WHERE p.status = $1'
                params.append(status)
            
            query += ' ORDER BY p.created_at DESC'
            
            payouts = await conn.fetch(query, *params)
            return [dict(payout) for payout in payouts]
    
    async def process_payout(self, payout_id: int, admin_id: int, approve: bool = True, rejection_reason: str = None) -> bool:
        """Обработать заявку на вывод"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                payout = await conn.fetchrow('SELECT * FROM payouts WHERE id = $1 AND status = $2', payout_id, 'pending')
                
                if not payout:
                    return False
                
                if approve:
                    # Выплата одобрена
                    await conn.execute('''
                        UPDATE payouts 
                        SET status = 'completed',
                            processed_at = CURRENT_TIMESTAMP,
                            processed_by = $1
                        WHERE id = $2
                    ''', admin_id, payout_id)
                    
                    # Убираем из pending_earnings
                    await conn.execute('''
                        UPDATE users 
                        SET pending_earnings = pending_earnings - $1
                        WHERE id = $2
                    ''', payout['amount'], payout['user_id'])
                    
                    # Уведомление пользователю
                    await self.create_notification(
                        'payout_approved',
                        payout['user_id'],
                        {
                            'payout_id': payout_id,
                            'amount': payout['amount'],
                            'processed_at': datetime.now().isoformat()
                        }
                    )
                    
                    # Логирование
                    await self.log_activity(admin_id, 'payout_approved', {
                        'payout_id': payout_id,
                        'user_id': payout['user_id'],
                        'amount': payout['amount']
                    })
                else:
                    # Выплата отклонена
                    await conn.execute('''
                        UPDATE payouts 
                        SET status = 'rejected',
                            processed_at = CURRENT_TIMESTAMP,
                            processed_by = $1,
                            rejection_reason = $2
                        WHERE id = $3
                    ''', admin_id, rejection_reason, payout_id)
                    
                    # Возвращаем деньги на баланс
                    await conn.execute('''
                        UPDATE users 
                        SET balance = balance + $1,
                            pending_earnings = pending_earnings - $1
                        WHERE id = $2
                    ''', payout['amount'], payout['user_id'])
                    
                    # Уведомление пользователю
                    await self.create_notification(
                        'payout_rejected',
                        payout['user_id'],
                        {
                            'payout_id': payout_id,
                            'amount': payout['amount'],
                            'reason': rejection_reason
                        }
                    )
                    
                    # Логирование
                    await self.log_activity(admin_id, 'payout_rejected', {
                        'payout_id': payout_id,
                        'user_id': payout['user_id'],
                        'amount': payout['amount'],
                        'reason': rejection_reason
                    })
                
                return True
    
    # ==================== ПОРТФОЛИО ====================
    
    async def add_portfolio_work(self, title: str, description: str, game_type: str, client_name: str, photos: List[str]) -> Optional[Dict]:
        """Добавить работу в портфолио"""
        async with self.pool.acquire() as conn:
            portfolio = await conn.fetchrow('''
                INSERT INTO portfolio (title, description, game_type, client_name, photos)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            ''', title, description, game_type, client_name, json.dumps(photos))
            
            return dict(portfolio) if portfolio else None
    
    async def get_portfolio(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        """Получить работы из портфолио"""
        async with self.pool.acquire() as conn:
            portfolio = await conn.fetch('''
                SELECT * FROM portfolio 
                WHERE status = 'published'
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            ''', limit, offset)
            
            return [dict(item) for item in portfolio]
    
    async def get_portfolio_item(self, item_id: int) -> Optional[Dict]:
        """Получить работу из портфолио по ID"""
        async with self.pool.acquire() as conn:
            item = await conn.fetchrow('SELECT * FROM portfolio WHERE id = $1', item_id)
            
            if item:
                # Увеличиваем счетчик просмотров
                await conn.execute('''
                    UPDATE portfolio SET views_count = views_count + 1 WHERE id = $1
                ''', item_id)
                
                # Парсим photos из JSON
                item_dict = dict(item)
                if item_dict['photos']:
                    item_dict['photos'] = json.loads(item_dict['photos'])
                else:
                    item_dict['photos'] = []
                
                return item_dict
            
            return None
    
    async def add_portfolio_review(self, portfolio_id: int, client_name: str, review_text: str, rating: int) -> bool:
        """Добавить отзыв к работе в портфолио"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Добавляем отзыв
                await conn.execute('''
                    INSERT INTO portfolio_reviews (portfolio_id, client_name, review_text, rating)
                    VALUES ($1, $2, $3, $4)
                ''', portfolio_id, client_name, review_text, rating)
                
                # Пересчитываем средний рейтинг
                avg_rating = await conn.fetchval('''
                    SELECT AVG(rating)::numeric(3,2) 
                    FROM portfolio_reviews 
                    WHERE portfolio_id = $1
                ''', portfolio_id)
                
                reviews_count = await conn.fetchval('''
                    SELECT COUNT(*) FROM portfolio_reviews WHERE portfolio_id = $1
                ''', portfolio_id)
                
                await conn.execute('''
                    UPDATE portfolio 
                    SET rating = $1, reviews_count = $2
                    WHERE id = $3
                ''', avg_rating or 0, reviews_count, portfolio_id)
                
                return True
    
    # ==================== УВЕДОМЛЕНИЯ ====================
    
    async def create_notification(self, notification_type: str, user_id: Optional[int], data: Dict, admin_only: bool = False) -> bool:
        """Создать уведомление"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO notifications (notification_type, user_id, admin_only, data)
                VALUES ($1, $2, $3, $4)
            ''', notification_type, user_id, admin_only, json.dumps(data))
            
            return True
    
    async def get_admin_notifications(self, limit: int = 20) -> List[Dict]:
        """Получить уведомления для админов"""
        async with self.pool.acquire() as conn:
            notifications = await conn.fetch('''
                SELECT n.*, u.full_name as user_name, u.username
                FROM notifications n
                LEFT JOIN users u ON n.user_id = u.id
                WHERE (n.admin_only = TRUE OR n.user_id IS NULL) AND n.is_read = FALSE
                ORDER BY n.created_at DESC
                LIMIT $1
            ''', limit)
            
            return [dict(notif) for notif in notifications]
    
    async def get_user_notifications(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Получить уведомления пользователя"""
        async with self.pool.acquire() as conn:
            notifications = await conn.fetch('''
                SELECT * FROM notifications 
                WHERE user_id = $1 AND is_read = FALSE AND admin_only = FALSE
                ORDER BY created_at DESC
                LIMIT $2
            ''', user_id, limit)
            
            return [dict(notif) for notif in notifications]
    
    async def mark_notification_read(self, notification_id: int) -> bool:
        """Пометить уведомление как прочитанное"""
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE notifications SET is_read = TRUE WHERE id = $1', notification_id)
            return True
    
    async def mark_all_notifications_read(self, user_id: int = None, admin_only: bool = False) -> bool:
        """Пометить все уведомления как прочитанные"""
        async with self.pool.acquire() as conn:
            query = 'UPDATE notifications SET is_read = TRUE WHERE is_read = FALSE'
            params = []
            
            if user_id:
                query += ' AND user_id = $1'
                params.append(user_id)
            elif admin_only:
                query += ' AND admin_only = TRUE'
            
            await conn.execute(query, *params)
            return True
    
    # ==================== СТАТИСТИКА ====================
    
    async def get_system_statistics(self, force_refresh: bool = False) -> Dict:
        """Получить системную статистику"""
        cache_key = 'system_stats'
        
        if not force_refresh and cache_key in self.stats_cache:
            cached_time, stats = self.stats_cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_timeout:
                return stats
        
        async with self.pool.acquire() as conn:
            today = datetime.now().date()
            week_ago = today - timedelta(days=7)
            month_ago = today - timedelta(days=30)
            
            # Основная статистика
            stats = await conn.fetchrow('''
                SELECT 
                    -- Пользователи
                    (SELECT COUNT(*) FROM users) as total_users,
                    (SELECT COUNT(*) FROM users WHERE created_at::date = $1) as new_users_today,
                    (SELECT COUNT(*) FROM users WHERE created_at >= $2) as new_users_week,
                    (SELECT COUNT(*) FROM users WHERE created_at >= $3) as new_users_month,
                    (SELECT COUNT(*) FROM users WHERE referrer_id IS NOT NULL) as referrers_count,
                    (SELECT COUNT(*) FROM users WHERE is_vip = TRUE) as vip_users_count,
                    (SELECT COUNT(*) FROM users WHERE last_active >= $2) as active_users_week,
                    
                    -- Заказы
                    (SELECT COUNT(*) FROM orders) as total_orders,
                    (SELECT COUNT(*) FROM orders WHERE created_at::date = $1) as new_orders_today,
                    (SELECT COUNT(*) FROM orders WHERE created_at >= $2) as new_orders_week,
                    (SELECT COUNT(*) FROM orders WHERE created_at >= $3) as new_orders_month,
                    (SELECT COUNT(*) FROM orders WHERE status = 'new') as pending_orders,
                    (SELECT COUNT(*) FROM orders WHERE status = 'active') as active_orders,
                    (SELECT COUNT(*) FROM orders WHERE status = 'completed') as completed_orders,
                    (SELECT COALESCE(SUM(price), 0) FROM orders WHERE status = 'completed') as orders_revenue,
                    (SELECT COALESCE(AVG(price), 0)::integer FROM orders WHERE price > 0) as avg_order_price,
                    
                    -- Финансы
                    (SELECT COALESCE(SUM(balance), 0) FROM users) as total_balance,
                    (SELECT COALESCE(SUM(pending_earnings), 0) FROM users) as pending_earnings,
                    (SELECT COALESCE(SUM(amount), 0) FROM payouts WHERE status = 'completed') as total_payouts,
                    (SELECT COALESCE(SUM(amount), 0) FROM payouts WHERE status = 'pending') as pending_payouts_amount,
                    (SELECT COUNT(*) FROM payouts WHERE status = 'pending') as pending_payouts_count,
                    
                    -- Консультации
                    (SELECT COUNT(*) FROM consultations) as total_consultations,
                    (SELECT COUNT(*) FROM consultations WHERE consultation_date = $1) as consultations_today,
                    (SELECT COUNT(*) FROM consultations WHERE consultation_date >= $2 AND consultation_date <= $1) as consultations_week,
                    (SELECT COUNT(*) FROM consultations WHERE status = 'pending') as pending_consultations,
                    (SELECT COUNT(*) FROM consultations WHERE status = 'confirmed') as confirmed_consultations,
                    (SELECT COALESCE(SUM(price), 0) FROM consultations WHERE payment_confirmed = TRUE) as consultations_revenue,
                    
                    -- Бонусы
                    (SELECT COUNT(*) FROM user_bonuses WHERE status = 'active') as active_bonuses,
                    (SELECT COUNT(*) FROM user_bonuses WHERE status = 'pending_review') as pending_bonuses,
                    (SELECT COUNT(*) FROM user_bonuses WHERE status = 'completed') as completed_bonuses,
                    (SELECT COALESCE(SUM(b.reward), 0) FROM user_bonuses ub JOIN bonuses b ON ub.bonus_id = b.id WHERE ub.status = 'completed') as bonuses_paid,
                    
                    -- Незавершенные заявки
                    (SELECT COUNT(*) FROM orders 
                     WHERE status = 'new' 
                     AND started_at < NOW() - INTERVAL '36 hours') as incomplete_orders_36h
            ''', today, week_ago, month_ago)
            
            # Топ пользователей по заказам
            top_users_orders = await conn.fetch('''
                SELECT u.full_name, u.username, COUNT(o.id) as order_count, COALESCE(SUM(o.price), 0) as total_spent
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'completed'
                GROUP BY u.id, u.full_name, u.username
                ORDER BY order_count DESC, total_spent DESC
                LIMIT 5
            ''')
            
            # Топ рефереров
            top_referrers = await conn.fetch('''
                SELECT u.full_name, u.username, COUNT(r.id) as referrals_count, COALESCE(SUM(u2.total_spent), 0) as referral_revenue
                FROM users u
                JOIN users r ON u.id = r.referrer_id
                LEFT JOIN users u2 ON r.id = u2.id
                GROUP BY u.id, u.full_name, u.username
                ORDER BY referrals_count DESC, referral_revenue DESC
                LIMIT 5
            ''')
            
            # Конверсия консультаций в заказы
            conversion_rate = await conn.fetchval('''
                SELECT 
                    CASE 
                        WHEN COUNT(*) > 0 THEN 
                            ROUND((COUNT(CASE WHEN conversion_to_order = TRUE THEN 1 END)::numeric / COUNT(*)) * 100, 1)
                        ELSE 0 
                    END
                FROM consultations 
                WHERE consultation_date < $1
            ''', today)
            
            result = {
                'basic': dict(stats) if stats else {},
                'top_users_orders': [dict(user) for user in top_users_orders],
                'top_referrers': [dict(ref) for ref in top_referrers],
                'conversion_rate': conversion_rate or 0,
                'calculated_at': datetime.now().isoformat()
            }
            
            # Кешируем результат
            self.stats_cache[cache_key] = (datetime.now(), result)
            
            return result
    
    async def get_user_statistics(self, user_id: int) -> Dict:
        """Получить статистику пользователя"""
        async with self.pool.acquire() as conn:
            # Основная статистика пользователя
            user_stats = await conn.fetchrow('''
                SELECT 
                    u.*,
                    (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as total_orders_count,
                    (SELECT COUNT(*) FROM orders WHERE user_id = u.id AND status = 'active') as active_orders_count,
                    (SELECT COUNT(*) FROM orders WHERE user_id = u.id AND status = 'completed') as completed_orders_count,
                    (SELECT COALESCE(SUM(price), 0) FROM orders WHERE user_id = u.id AND status = 'completed') as total_spent_amount,
                    (SELECT COUNT(*) FROM consultations WHERE user_id = u.id) as consultations_count,
                    (SELECT COUNT(*) FROM consultations WHERE user_id = u.id AND conversion_to_order = TRUE) as converted_consultations_count,
                    (SELECT COUNT(*) FROM users WHERE referrer_id = u.id) as referrals_count,
                    (SELECT COALESCE(SUM(total_spent), 0) FROM users WHERE referrer_id = u.id) as referral_revenue,
                    (SELECT COUNT(*) FROM user_bonuses WHERE user_id = u.id AND status = 'completed') as completed_bonuses_count,
                    (SELECT COALESCE(SUM(b.reward), 0) FROM user_bonuses ub JOIN bonuses b ON ub.bonus_id = b.id WHERE ub.user_id = u.id AND ub.status = 'completed') as bonuses_earned
                FROM users u
                WHERE u.id = $1
            ''', user_id)
            
            # История активности
            activity_history = await conn.fetch('''
                SELECT action_type, details, created_at 
                FROM activity_log 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT 10
            ''', user_id)
            
            # История заказов
            order_history = await conn.fetch('''
                SELECT order_number, game_name, status, price, created_at 
                FROM orders 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT 5
            ''', user_id)
            
            # Активные бонусы
            active_bonuses = await conn.fetch('''
                SELECT ub.*, b.name, b.icon 
                FROM user_bonuses ub
                JOIN bonuses b ON ub.bonus_id = b.id
                WHERE ub.user_id = $1 AND ub.status = 'active'
                ORDER BY ub.end_date
            ''', user_id)
            
            return {
                'user_stats': dict(user_stats) if user_stats else {},
                'activity_history': [dict(activity) for activity in activity_history],
                'order_history': [dict(order) for order in order_history],
                'active_bonuses': [dict(bonus) for bonus in active_bonuses]
            }
    
    async def get_daily_report_data(self) -> Dict:
        """Получить данные для ежедневного отчета"""
        async with self.pool.acquire() as conn:
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            report = await conn.fetchrow('''
                SELECT 
                    -- Новые пользователи
                    (SELECT COUNT(*) FROM users WHERE created_at::date = $1) as new_users_yesterday,
                    (SELECT COUNT(*) FROM users WHERE created_at::date = $2) as new_users_today,
                    
                    -- Новые заказы
                    (SELECT COUNT(*) FROM orders WHERE created_at::date = $1) as new_orders_yesterday,
                    (SELECT COUNT(*) FROM orders WHERE created_at::date = $2) as new_orders_today,
                    
                    -- Выручка
                    (SELECT COALESCE(SUM(price), 0) FROM orders WHERE created_at::date = $1 AND status = 'completed') as revenue_yesterday,
                    (SELECT COALESCE(SUM(price), 0) FROM orders WHERE created_at::date = $2 AND status = 'completed') as revenue_today,
                    
                    -- Консультации
                    (SELECT COUNT(*) FROM consultations WHERE consultation_date = $1) as consultations_yesterday,
                    (SELECT COUNT(*) FROM consultations WHERE consultation_date = $2) as consultations_today,
                    
                    -- Выплаты
                    (SELECT COUNT(*) FROM payouts WHERE created_at::date = $1) as payouts_yesterday,
                    (SELECT COUNT(*) FROM payouts WHERE created_at::date = $2) as payouts_today,
                    (SELECT COALESCE(SUM(amount), 0) FROM payouts WHERE created_at::date = $1) as payouts_amount_yesterday,
                    (SELECT COALESCE(SUM(amount), 0) FROM payouts WHERE created_at::date = $2) as payouts_amount_today,
                    
                    -- Незавершенные заявки
                    (SELECT COUNT(*) FROM orders 
                     WHERE status = 'new' 
                     AND started_at < NOW() - INTERVAL '36 hours') as incomplete_orders,
                     
                    -- Неподтвержденные оплаты
                    (SELECT COUNT(*) FROM consultations 
                     WHERE payment_confirmed = FALSE 
                     AND status = 'pending'
                     AND created_at < NOW() - INTERVAL '3 hours') as unpaid_consultations,
                     
                    -- Требующие внимания
                    (SELECT COUNT(*) FROM notifications 
                     WHERE admin_only = TRUE AND is_read = FALSE) as unread_notifications
            ''', yesterday, today)
            
            # Запланированные на сегодня консультации
            todays_consultations = await conn.fetch('''
                SELECT c.*, u.full_name, u.phone 
                FROM consultations c
                JOIN users u ON c.user_id = u.id
                WHERE c.consultation_date = $1
                ORDER BY c.consultation_time
            ''', today)
            
            # Дедлайны заказов на ближайшие 3 дня
            upcoming_deadlines = await conn.fetch('''
                SELECT o.*, u.full_name 
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.deadline BETWEEN $1 AND $1 + INTERVAL '3 days'
                AND o.status IN ('new', 'active')
                ORDER BY o.deadline
            ''', today)
            
            # Выплаты к обработке
            pending_payouts = await conn.fetch('''
                SELECT p.*, u.full_name 
                FROM payouts p
                JOIN users u ON p.user_id = u.id
                WHERE p.status = 'pending'
                ORDER BY p.created_at
            ''')
            
            return {
                'report': dict(report) if report else {},
                'todays_consultations': [dict(cons) for cons in todays_consultations],
                'upcoming_deadlines': [dict(deadline) for deadline in upcoming_deadlines],
                'pending_payouts': [dict(payout) for payout in pending_payouts],
                'report_date': today.strftime('%d.%m.%Y')
            }
    
    # ==================== РАССЫЛКИ ====================
    
    async def create_mailing(self, title: str, message: str, audience_type: str, filters: Dict = None) -> Optional[Dict]:
        """Создать рассылку"""
        async with self.pool.acquire() as conn:
            mailing_number = f"MAIL{datetime.now().strftime('%y%m%d')}{uuid.uuid4().hex[:4].upper()}"
            
            mailing = await conn.fetchrow('''
                INSERT INTO mailings (mailing_number, title, message, audience_type, filters)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            ''', mailing_number, title, message, audience_type, json.dumps(filters or {}))
            
            return dict(mailing) if mailing else None
    
    async def get_mailing_recipients(self, mailing_id: int) -> List[Dict]:
        """Получить получателей рассылки"""
        async with self.pool.acquire() as conn:
            mailing = await conn.fetchrow('SELECT * FROM mailings WHERE id = $1', mailing_id)
            if not mailing:
                return []
            
            mailing_dict = dict(mailing)
            filters = json.loads(mailing_dict['filters']) if mailing_dict['filters'] else {}
            
            # Формируем запрос в зависимости от типа аудитории
            query = 'SELECT telegram_id, full_name, username FROM users WHERE 1=1'
            params = []
            
            if mailing_dict['audience_type'] == 'all':
                # Все пользователи
                pass
            elif mailing_dict['audience_type'] == 'with_orders':
                # Пользователи с заказами
                query += ' AND id IN (SELECT DISTINCT user_id FROM orders)'
            elif mailing_dict['audience_type'] == 'with_balance':
                # Пользователи с балансом
                query += ' AND balance > 0'
            elif mailing_dict['audience_type'] == 'referrers':
                # Рефереры
                query += ' AND id IN (SELECT DISTINCT referrer_id FROM users WHERE referrer_id IS NOT NULL)'
            elif mailing_dict['audience_type'] == 'vip':
                # ВИП-клиенты
                query += ' AND is_vip = TRUE'
            
            # Применяем дополнительные фильтры
            if filters.get('min_orders'):
                query += ' AND total_orders >= $' + str(len(params) + 1)
                params.append(filters['min_orders'])
            
            if filters.get('min_balance'):
                query += ' AND balance >= $' + str(len(params) + 1)
                params.append(filters['min_balance'])
            
            recipients = await conn.fetch(query, *params)
            return [dict(recipient) for recipient in recipients]
    
    async def update_mailing_stats(self, mailing_id: int, sent_count: int = 0, read_count: int = 0) -> bool:
        """Обновить статистику рассылки"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE mailings 
                SET sent_count = sent_count + $1,
                    read_count = read_count + $2,
                    total_recipients = (SELECT COUNT(*) FROM users WHERE telegram_id IS NOT NULL)
                WHERE id = $3
            ''', sent_count, read_count, mailing_id)
            
            return True
    
    async def get_mailings(self, limit: int = 10) -> List[Dict]:
        """Получить список рассылок"""
        async with self.pool.acquire() as conn:
            mailings = await conn.fetch('''
                SELECT * FROM mailings 
                ORDER BY created_at DESC 
                LIMIT $1
            ''', limit)
            
            return [dict(mailing) for mailing in mailings]
    
    # ==================== СИСТЕМНЫЕ НАСТРОЙКИ ====================
    
    async def get_system_setting(self, key: str) -> Optional[str]:
        """Получить значение системной настройки"""
        async with self.pool.acquire() as conn:
            value = await conn.fetchval('SELECT value FROM system_settings WHERE key = $1', key)
            return value
    
    async def update_system_setting(self, key: str, value: str) -> bool:
        """Обновить системную настройку"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
            ''', key, value)
            
            # Очищаем кеш статистики
            if 'system_stats' in self.stats_cache:
                del self.stats_cache['system_stats']
            
            return True
    
    async def get_all_settings(self) -> Dict[str, str]:
        """Получить все системные настройки"""
        async with self.pool.acquire() as conn:
            settings = await conn.fetch('SELECT key, value FROM system_settings')
            return {setting['key']: setting['value'] for setting in settings}
    
    # ==================== АДМИНИСТРАТОРЫ ====================
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверить, является ли пользователь администратором"""
        async with self.pool.acquire() as conn:
            is_admin = await conn.fetchval('SELECT 1 FROM admins WHERE user_id = $1', user_id)
            return bool(is_admin)
    
    async def get_admin_username(self, admin_id: int) -> Optional[str]:
        """Получить username администратора"""
        async with self.pool.acquire() as conn:
            username = await conn.fetchval('SELECT username FROM users WHERE id = $1', admin_id)
            return username
    
    async def add_admin(self, user_id: int, added_by: int, permissions: List[str] = None) -> bool:
        """Добавить администратора"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO admins (user_id, added_by, permissions)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET
                    permissions = EXCLUDED.permissions,
                    added_at = CURRENT_TIMESTAMP
            ''', user_id, added_by, json.dumps(permissions or ['all']))
            
            await self.log_activity(added_by, 'admin_added', {'added_user_id': user_id})
            return True
    
    async def remove_admin(self, user_id: int, removed_by: int) -> bool:
        """Удалить администратора"""
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
            await self.log_activity(removed_by, 'admin_removed', {'removed_user_id': user_id})
            return True
    
    async def get_admins(self) -> List[Dict]:
        """Получить список администраторов"""
        async with self.pool.acquire() as conn:
            admins = await conn.fetch('''
                SELECT a.*, u.full_name, u.username, u.telegram_id
                FROM admins a
                JOIN users u ON a.user_id = u.id
                ORDER BY a.added_at
            ''')
            
            return [dict(admin) for admin in admins]
    
    # ==================== ЧЕКИ ====================
    
    async def create_receipt(self, user_id: int, amount: int, payment_type: str, receipt_data: str, order_id: int = None, consultation_id: int = None) -> Optional[Dict]:
        """Создать запись о чеке"""
        async with self.pool.acquire() as conn:
            receipt_number = f"REC{datetime.now().strftime('%y%m%d')}{uuid.uuid4().hex[:4].upper()}"
            
            receipt = await conn.fetchrow('''
                INSERT INTO receipts (receipt_number, user_id, amount, payment_type, receipt_data, order_id, consultation_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING *
            ''', receipt_number, user_id, amount, payment_type, receipt_data, order_id, consultation_id)
            
            # Уведомление админам
            await self.create_notification(
                'new_receipt',
                None,
                {
                    'receipt_id': receipt['id'],
                    'receipt_number': receipt_number,
                    'user_id': user_id,
                    'amount': amount,
                    'payment_type': payment_type,
                    'order_id': order_id,
                    'consultation_id': consultation_id
                },
                admin_only=True
            )
            
            return dict(receipt) if receipt else None
    
    async def get_receipts(self, confirmed: bool = None) -> List[Dict]:
        """Получить чеки"""
        async with self.pool.acquire() as conn:
            query = '''
                SELECT r.*, u.full_name, u.username,
                       o.order_number, c.consultation_number
                FROM receipts r
                JOIN users u ON r.user_id = u.id
                LEFT JOIN orders o ON r.order_id = o.id
                LEFT JOIN consultations c ON r.consultation_id = c.id
            '''
            params = []
            
            if confirmed is not None:
                query += ' WHERE r.confirmed = $1'
                params.append(confirmed)
            
            query += ' ORDER BY r.created_at DESC'
            
            receipts = await conn.fetch(query, *params)
            return [dict(receipt) for receipt in receipts]
    
    async def confirm_receipt(self, receipt_id: int, admin_id: int) -> bool:
        """Подтвердить чек"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                receipt = await conn.fetchrow('''
                    UPDATE receipts 
                    SET confirmed = TRUE,
                        confirmed_by = $1,
                        confirmed_at = CURRENT_TIMESTAMP
                    WHERE id = $2 AND confirmed = FALSE
                    RETURNING *
                ''', admin_id, receipt_id)
                
                if not receipt:
                    return False
                
                # Если чек для консультации, подтверждаем оплату
                if receipt['consultation_id']:
                    await self.confirm_consultation_payment(receipt['consultation_id'], admin_id)
                
                # Если чек для заказа, обновляем оплату заказа
                elif receipt['order_id']:
                    await conn.execute('''
                        UPDATE orders 
                        SET paid_amount = paid_amount + $1,
                            last_activity = CURRENT_TIMESTAMP
                        WHERE id = $2
                    ''', receipt['amount'], receipt['order_id'])
                    
                    # Проверяем реферала для начисления бонусов
                    order = await self.get_order(receipt['order_id'])
                    if order and order['user_id']:
                        user = await self.get_user_by_id(order['user_id'])
                        if user and user['referrer_id']:
                            # Начисляем фиксированный бонус рефереру
                            referral_bonus = int(await self.get_system_setting('referral_bonus') or 400)
                            await self.update_user_balance(
                                user['referrer_id'],
                                referral_bonus,
                                'referral_bonus',
                                {'referral_id': user['id'], 'order_id': receipt['order_id']}
                            )
                            
                            # Начисляем процент от заказа
                            referral_percentage = int(await self.get_system_setting('referral_percentage') or 10)
                            percentage_bonus = int(receipt['amount'] * referral_percentage / 100)
                            
                            if percentage_bonus > 0:
                                await self.update_user_balance(
                                    user['referrer_id'],
                                    percentage_bonus,
                                    'referral_percentage',
                                    {'referral_id': user['id'], 'order_id': receipt['order_id'], 'amount': receipt['amount'], 'percentage': referral_percentage}
                                )
                
                # Уведомление пользователю
                await self.create_notification(
                    'receipt_confirmed',
                    receipt['user_id'],
                    {
                        'receipt_id': receipt_id,
                        'amount': receipt['amount'],
                        'payment_type': receipt['payment_type']
                    }
                )
                
                await self.log_activity(admin_id, 'receipt_confirmed', {'receipt_id': receipt_id})
                return True
    
    async def reject_receipt(self, receipt_id: int, admin_id: int, reason: str) -> bool:
        """Отклонить чек"""
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM receipts WHERE id = $1', receipt_id)
            
            # Уведомление админам о удалении
            await self.log_activity(admin_id, 'receipt_rejected', {
                'receipt_id': receipt_id,
                'reason': reason
            })
            
            return True
    
    # ==================== АВТОМАТИЧЕСКИЕ ЗАДАЧИ ====================
    
    async def check_incomplete_orders(self):
        """Проверить незавершенные заявки (через 36 часов)"""
        async with self.pool.acquire() as conn:
            incomplete_orders = await conn.fetch('''
                SELECT o.*, u.telegram_id, u.full_name
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.status = 'new'
                AND o.started_at < NOW() - INTERVAL '36 hours'
                AND o.last_activity < NOW() - INTERVAL '1 hour'
            ''')
            
            for order in incomplete_orders:
                order_dict = dict(order)
                
                # Отправляем напоминание пользователю
                try:
                    reminder_text = """⏰ Вы не завершили оформление заявки

Завершите оформление заявки в течение 24 часов и получите скидку 7% на заказ!"""
                    
                    keyboard = InlineKeyboardMarkup()
                    keyboard.add(
                        InlineKeyboardButton("🚀 Продолжить оформление", callback_data=f"continue_order_{order_dict['id']}"),
                        InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
                    )
                    
                    await bot.send_message(
                        order_dict['telegram_id'],
                        reminder_text,
                        reply_markup=keyboard
                    )
                    
                    # Обновляем время последней активности
                    await conn.execute('''
                        UPDATE orders SET last_activity = CURRENT_TIMESTAMP WHERE id = $1
                    ''', order_dict['id'])
                    
                    # Логируем отправку напоминания
                    await self.log_activity(order_dict['user_id'], 'incomplete_order_reminder', {
                        'order_id': order_dict['id'],
                        'order_number': order_dict['order_number']
                    })
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки напоминания для заказа {order_dict['id']}: {e}")
    
    async def send_consultation_reminders(self):
        """Отправить напоминания о консультациях (за 24 часа)"""
        async with self.pool.acquire() as conn:
            tomorrow = (datetime.now() + timedelta(days=1)).date()
            
            consultations = await conn.fetch('''
                SELECT c.*, u.telegram_id, u.full_name, u.username,
                       us.consultation_reminders
                FROM consultations c
                JOIN users u ON c.user_id = u.id
                LEFT JOIN user_settings us ON u.id = us.user_id
                WHERE c.consultation_date = $1
                AND c.status = 'confirmed'
                AND c.reminder_sent = FALSE
                AND (us.consultation_reminders IS NULL OR us.consultation_reminders = TRUE)
            ''', tomorrow)
            
            for consultation in consultations:
                consultation_dict = dict(consultation)
                
                try:
                    reminder_text = f"""🔔 Напоминание: Завтра в {consultation_dict['consultation_time'].strftime('%H:%M')} у вас консультация

Подготовка к консультации:
• Запишите все вопросы заранее
• Подготовьте примеры игр, которые вам нравятся
• Продумайте целевую аудиторию

Детали:
📅 Дата: {consultation_dict['consultation_date'].strftime('%d.%m.%Y')}
🕐 Время: {consultation_dict['consultation_time'].strftime('%H:%M')}
⏱️ Длительность: {consultation_dict['duration']} минут"""
                    
                    keyboard = InlineKeyboardMarkup()
                    keyboard.add(
                        InlineKeyboardButton("✏️ Перенести", callback_data=f"reschedule_consultation_{consultation_dict['id']}"),
                        InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_consultation_{consultation_dict['id']}"),
                        InlineKeyboardButton("💬 Написать эксперту", callback_data="contact_manager")
                    )
                    
                    await bot.send_message(
                        consultation_dict['telegram_id'],
                        reminder_text,
                        reply_markup=keyboard
                    )
                    
                    # Помечаем как отправленное
                    await conn.execute('''
                        UPDATE consultations 
                        SET reminder_sent = TRUE 
                        WHERE id = $1
                    ''', consultation_dict['id'])
                    
                    # Логируем отправку напоминания
                    await self.log_activity(consultation_dict['user_id'], 'consultation_reminder_sent', {
                        'consultation_id': consultation_dict['id'],
                        'consultation_number': consultation_dict['consultation_number']
                    })
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки напоминания о консультации {consultation_dict['id']}: {e}")
    
    async def send_daily_report(self):
        """Отправить ежедневный отчет админам"""
        try:
            report_data = await self.get_daily_report_data()
            
            report_text = f"""📊 ЕЖЕДНЕВНЫЙ ОТЧЁТ

Дата: {report_data['report_date']}

Статистика за вчера:
• 👥 Новые пользователи: {report_data['report'].get('new_users_yesterday', 0)}
• 📦 Новые заказы: {report_data['report'].get('new_orders_yesterday', 0)}
• 💰 Выручка: {report_data['report'].get('revenue_yesterday', 0)}₽
• 💬 Новые консультации: {report_data['report'].get('consultations_yesterday', 0)}
• 💳 Выплаты: {report_data['report'].get('payouts_yesterday', 0)} на {report_data['report'].get('payouts_amount_yesterday', 0)}₽

На сегодня запланировано:
• 📅 Консультаций: {report_data['report'].get('consultations_today', 0)}
• 📦 Дедлайнов по заказам: {len(report_data['upcoming_deadlines'])}
• 💳 Выплат к обработке: {len(report_data['pending_payouts'])}

Требует внимания:
⚠️ Неотвеченных заявок: {report_data['report'].get('incomplete_orders', 0)}
⚠️ Неподтверждённых оплат: {report_data['report'].get('unpaid_consultations', 0)}
⚠️ Незавершённых анкет: {report_data['report'].get('incomplete_orders', 0)}"""
            
            keyboard = InlineKeyboardMarkup(row_width=2)
            keyboard.add(
                InlineKeyboardButton("📦 К заказам", callback_data="admin_orders"),
                InlineKeyboardButton("👥 К пользователям", callback_data="admin_users")
            )
            keyboard.add(
                InlineKeyboardButton("📅 К консультациям", callback_data="admin_consultations"),
                InlineKeyboardButton("💳 К выплатам", callback_data="admin_payouts")
            )
            keyboard.add(InlineKeyboardButton("📈 Подробная статистика", callback_data="admin_stats"))
            
            # Отправляем отчет всем админам
            admins = await self.get_admins()
            for admin in admins:
                try:
                    await bot.send_message(
                        admin['telegram_id'],
                        report_text,
                        reply_markup=keyboard
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки отчета админу {admin['telegram_id']}: {e}")
            
            logger.info("Ежедневный отчет отправлен админам")
            
        except Exception as e:
            logger.error(f"Ошибка формирования ежедневного отчета: {e}")
    
    async def check_order_deadlines(self):
        """Проверить дедлайны заказов (за 3 дня)"""
        async with self.pool.acquire() as conn:
            three_days_later = (datetime.now() + timedelta(days=3)).date()
            
            orders = await conn.fetch('''
                SELECT o.*, u.telegram_id, u.full_name, m.username as manager_username
                FROM orders o
                JOIN users u ON o.user_id = u.id
                LEFT JOIN users m ON o.manager_id = m.id
                WHERE o.deadline = $1
                AND o.status IN ('new', 'active')
            ''', three_days_later)
            
            for order in orders:
                order_dict = dict(order)
                
                # Уведомление админам
                await self.create_notification(
                    'order_deadline_approaching',
                    None,
                    {
                        'order_id': order_dict['id'],
                        'order_number': order_dict['order_number'],
                        'deadline': order_dict['deadline'].strftime('%d.%m.%Y'),
                        'user_name': order_dict['full_name'],
                        'manager_username': order_dict['manager_username']
                    },
                    admin_only=True
                )
    
    # ==================== ЛОГИРОВАНИЕ ====================
    
    async def log_activity(self, user_id: Optional[int], action_type: str, details: Dict = None):
        """Записать действие в лог"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO activity_log (user_id, action_type, details)
                VALUES ($1, $2, $3)
            ''', user_id, action_type, json.dumps(details or {}))
            
            # Обновляем last_active для пользователя
            if user_id:
                await conn.execute('''
                    UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE id = $1
                ''', user_id)
    
    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================
    
    async def get_user_referral_stats(self, user_id: int) -> Dict:
        """Получить реферальную статистику пользователя"""
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    u.referral_code,
                    (SELECT COUNT(*) FROM users WHERE referrer_id = u.id) as total_referrals,
                    (SELECT COUNT(*) FROM users r 
                     JOIN orders o ON r.id = o.user_id 
                     WHERE r.referrer_id = u.id AND o.status = 'completed') as active_referrals,
                    (SELECT COALESCE(SUM(o.price), 0) FROM users r 
                     JOIN orders o ON r.id = o.user_id 
                     WHERE r.referrer_id = u.id AND o.status = 'completed') as referral_revenue,
                    (SELECT COALESCE(SUM(amount), 0) FROM payouts 
                     WHERE user_id = u.id AND status = 'completed') as total_paid,
                    u.balance as current_balance,
                    u.pending_earnings
                FROM users u
                WHERE u.id = $1
            ''', user_id)
            
            return dict(stats) if stats else {}
    
    async def export_statistics(self, export_type: str) -> str:
        """Экспорт статистики в CSV"""
        async with self.pool.acquire() as conn:
            output = io.StringIO()
            writer = csv.writer(output)
            
            if export_type == 'users':
                # Экспорт пользователей
                users = await conn.fetch('''
                    SELECT 
                        u.id, u.telegram_id, u.username, u.full_name, u.phone, u.email,
                        u.city, u.event_date, u.balance, u.total_earned, u.total_orders,
                        u.total_spent, u.is_vip, u.created_at,
                        (SELECT COUNT(*) FROM users WHERE referrer_id = u.id) as referrals_count
                    FROM users u
                    ORDER BY u.created_at DESC
                ''')
                
                writer.writerow(['ID', 'Telegram ID', 'Username', 'Полное имя', 'Телефон', 'Email',
                                'Город', 'Дата мероприятия', 'Баланс', 'Всего заработано', 'Всего заказов',
                                'Всего потрачено', 'ВИП', 'Дата регистрации', 'Рефералов'])
                
                for user in users:
                    writer.writerow([
                        user['id'], user['telegram_id'], user['username'], user['full_name'],
                        user['phone'], user['email'], user['city'], 
                        user['event_date'].strftime('%d.%m.%Y') if user['event_date'] else '',
                        user['balance'], user['total_earned'], user['total_orders'],
                        user['total_spent'], 'Да' if user['is_vip'] else 'Нет',
                        user['created_at'].strftime('%d.%m.%Y %H:%M'),
                        user['referrals_count']
                    ])
            
            elif export_type == 'orders':
                # Экспорт заказов
                orders = await conn.fetch('''
                    SELECT 
                        o.id, o.order_number, u.full_name, u.phone, u.email,
                        o.game_name, o.occasion, o.target_audience, o.budget,
                        o.players_count, o.price, o.paid_amount, o.status,
                        o.current_stage, o.total_stages, o.progress_percent,
                        o.deadline, o.created_at
                    FROM orders o
                    JOIN users u ON o.user_id = u.id
                    ORDER BY o.created_at DESC
                ''')
                
                writer.writerow(['ID', 'Номер заказа', 'Клиент', 'Телефон', 'Email',
                                'Название игры', 'Повод', 'Для кого', 'Бюджет',
                                'Игроков', 'Цена', 'Оплачено', 'Статус',
                                'Текущий этап', 'Всего этапов', 'Прогресс %',
                                'Дедлайн', 'Дата создания'])
                
                for order in orders:
                    writer.writerow([
                        order['id'], order['order_number'], order['full_name'],
                        order['phone'], order['email'], order['game_name'],
                        order['occasion'], order['target_audience'], order['budget'],
                        order['players_count'], order['price'], order['paid_amount'],
                        order['status'], order['current_stage'], order['total_stages'],
                        order['progress_percent'],
                        order['deadline'].strftime('%d.%m.%Y') if order['deadline'] else '',
                        order['created_at'].strftime('%d.%m.%Y %H:%M')
                    ])
            
            elif export_type == 'consultations':
                # Экспорт консультаций
                consultations = await conn.fetch('''
                    SELECT 
                        c.id, c.consultation_number, u.full_name, u.phone, u.email,
                        c.consultation_date, c.consultation_time, c.duration, c.price,
                        c.paid_amount, c.status, c.payment_confirmed, c.conversion_to_order,
                        c.created_at
                    FROM consultations c
                    JOIN users u ON c.user_id = u.id
                    ORDER BY c.consultation_date DESC, c.consultation_time DESC
                ''')
                
                writer.writerow(['ID', 'Номер', 'Клиент', 'Телефон', 'Email',
                                'Дата', 'Время', 'Длительность (мин)', 'Цена',
                                'Оплачено', 'Статус', 'Оплата подтверждена', 'Конвертировался в заказ',
                                'Дата создания'])
                
                for consultation in consultations:
                    writer.writerow([
                        consultation['id'], consultation['consultation_number'],
                        consultation['full_name'], consultation['phone'], consultation['email'],
                        consultation['consultation_date'].strftime('%d.%m.%Y'),
                        consultation['consultation_time'].strftime('%H:%M'),
                        consultation['duration'], consultation['price'],
                        consultation['paid_amount'], consultation['status'],
                        'Да' if consultation['payment_confirmed'] else 'Нет',
                        'Да' if consultation['conversion_to_order'] else 'Нет',
                        consultation['created_at'].strftime('%d.%m.%Y %H:%M')
                    ])
            
            output.seek(0)
            return output.getvalue()
    
    async def close(self):
        """Закрыть соединение с базой данных"""
        if self.pool:
            await self.pool.close()
            logger.info("Соединение с БД закрыто")

# Создаем глобальный экземпляр базы данных
db = Database()

# ==================== STATES (СОСТОЯНИЯ FSM) ====================

class OrderForm(StatesGroup):
    step1_name = State()
    step2_phone = State()
    step3_date = State()
    step4_target = State()
    step5_budget = State()
    step6_players = State()
    step7_emotions = State()
    step8_basis = State()
    step9_source = State()
    step10_frequency = State()
    step11_description = State()
    step12_telegram = State()

class ProfileEditForm(StatesGroup):
    edit_name = State()
    edit_phone = State()
    edit_email = State()
    edit_city = State()
    edit_event_date = State()

class ConsultationForm(StatesGroup):
    choose_date = State()
    choose_time = State()
    payment = State()

class PayoutForm(StatesGroup):
    enter_amount = State()
    enter_card = State()
    enter_card_holder = State()

class AdminStates(StatesGroup):
    add_consultation_slot_date = State()
    add_consultation_slot_time = State()
    add_portfolio_title = State()
    add_portfolio_description = State()
    add_portfolio_game_type = State()
    add_portfolio_client = State()
    add_portfolio_photos = State()
    edit_setting_select = State()
    edit_setting_value = State()
    create_bonus_name = State()
    create_bonus_description = State()
    create_bonus_reward = State()
    create_bonus_conditions = State()
    send_mailing_title = State()
    send_mailing_message = State()
    send_mailing_audience = State()

class ReceiptForm(StatesGroup):
    enter_amount = State()
    enter_type = State()
    upload_receipt = State()

# ==================== КЛАВИАТУРЫ ====================

def get_main_menu_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Клавиатура главного меню"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🎮 Оформить заявку"),
        KeyboardButton("❓ Помощь")
    )
    keyboard.add(
        KeyboardButton("📞 Контакты"),
        KeyboardButton("👤 Мой профиль")
    )
    if is_admin:
        keyboard.add(KeyboardButton("👑 Админ"))
    return keyboard

def get_help_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура помощи"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🎮 Создание заказа", callback_data="help_order"),
        InlineKeyboardButton("💬 Консультация", callback_data="help_consultation")
    )
    keyboard.add(
        InlineKeyboardButton("📞 Поддержка", callback_data="help_support"),
        InlineKeyboardButton("🕐 Время работы", callback_data="help_schedule")
    )
    return keyboard

def get_back_to_help_keyboard() -> InlineKeyboardMarkup:
    """Кнопка назад в помощь"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад в помощь", callback_data="back_to_help"))
    return keyboard

def get_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    """Кнопка назад в меню"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu"))
    return keyboard

def get_order_start_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура начала оформления заказа"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🚀 Начать оформление"),
        KeyboardButton("🔙 Главное меню")
    )
    return keyboard

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопка отмены"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_emotions_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора эмоций (шаг 7)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("😄 Веселье и смех", callback_data="emotion_fun"),
        InlineKeyboardButton("🥰 Тепло и ностальгия", callback_data="emotion_warmth")
    )
    keyboard.add(
        InlineKeyboardButton("😱 Азарт и соперничество", callback_data="emotion_excitement"),
        InlineKeyboardButton("🤔 Стратегия и мысли", callback_data="emotion_strategy")
    )
    keyboard.add(
        InlineKeyboardButton("🤝 Командный дух", callback_data="emotion_team"),
        InlineKeyboardButton("✨ Другое", callback_data="emotion_other")
    )
    keyboard.add(InlineKeyboardButton("👉 Далее", callback_data="emotions_next"))
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_target_audience_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора целевой аудитории (шаг 4)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("👨‍👩‍👧‍👦 Для семьи", callback_data="target_family"),
        InlineKeyboardButton("👫 Для второй половинки", callback_data="target_couple")
    )
    keyboard.add(
        InlineKeyboardButton("🏢 Для команды / Коллег", callback_data="target_team"),
        InlineKeyboardButton("🤝 Для друга", callback_data="target_friend")
    )
    keyboard.add(InlineKeyboardButton("✨ Другое", callback_data="target_other"))
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_budget_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора бюджета (шаг 5)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("До 5.000₽", callback_data="budget_5000"),
        InlineKeyboardButton("До 10.000₽", callback_data="budget_10000")
    )
    keyboard.add(
        InlineKeyboardButton("До 20.000₽", callback_data="budget_20000"),
        InlineKeyboardButton("+20.000₽", callback_data="budget_20000plus")
    )
    keyboard.add(InlineKeyboardButton("💎 Другое", callback_data="budget_other"))
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_players_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора количества игроков (шаг 6)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("2-6 игроков", callback_data="players_2_6"),
        InlineKeyboardButton("6-12 игроков", callback_data="players_6_12")
    )
    keyboard.add(
        InlineKeyboardButton("12+ игроков", callback_data="players_12plus"),
        InlineKeyboardButton("🎯 Другое", callback_data="players_other")
    )
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_source_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора источника (шаг 9)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📱 Соцсети", callback_data="source_social"),
        InlineKeyboardButton("👤 Реферальная система", callback_data="source_referral")
    )
    keyboard.add(
        InlineKeyboardButton("🤝 Рекомендация друзей", callback_data="source_friends"),
        InlineKeyboardButton("📢 Реклама в Telegram", callback_data="source_telegram")
    )
    keyboard.add(InlineKeyboardButton("💼 Другое", callback_data="source_other"))
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_frequency_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора частоты игры (шаг 10)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🎲 Не играю, но хочу начать", callback_data="frequency_never"),
        InlineKeyboardButton("🤏 Редко, по особым случаям", callback_data="frequency_rare")
    )
    keyboard.add(
        InlineKeyboardButton("👨‍👩‍👧‍👦 Регулярно, это семейная традиция", callback_data="frequency_regular"),
        InlineKeyboardButton("🏆 Часто, я настоящий профессионал!", callback_data="frequency_often")
    )
    keyboard.add(InlineKeyboardButton("✨ Другое", callback_data="frequency_other"))
    keyboard.add(InlineKeyboardButton("❌ Отменить создание", callback_data="cancel_order"))
    return keyboard

def get_order_complete_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура после завершения анкеты"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📱 Связь с менеджером", callback_data="contact_manager"),
        InlineKeyboardButton("🖼️ Посмотреть портфолио", callback_data="view_portfolio")
    )
    keyboard.add(
        InlineKeyboardButton("💬 Консультация", callback_data="book_consultation"),
        InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )
    return keyboard

def get_profile_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура профиля"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("💰 Баланс", callback_data="profile_balance"),
        InlineKeyboardButton("👥 Рефералы", callback_data="profile_referrals")
    )
    keyboard.add(
        InlineKeyboardButton("📊 Статистика", callback_data="profile_stats"),
        InlineKeyboardButton("⚙️ Настройки", callback_data="profile_settings")
    )
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu"))
    return keyboard

def get_balance_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура баланса"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("💳 Вывести средства", callback_data="balance_withdraw"),
        InlineKeyboardButton("💳 Мои карты", callback_data="balance_cards")
    )
    keyboard.add(
        InlineKeyboardButton("📊 Подробнее", callback_data="balance_details"),
        InlineKeyboardButton("🎁 Бонусы", callback_data="balance_bonuses")
    )
    keyboard.add(InlineKeyboardButton("🔙 В профиль", callback_data="profile_menu"))
    return keyboard

def get_referrals_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура рефералов"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📱 Поделиться ссылкой", callback_data="referral_share"),
        InlineKeyboardButton("📊 Детальная статистика", callback_data="referral_stats")
    )
    keyboard.add(
        InlineKeyboardButton("❓ Как приглашать?", callback_data="referral_howto"),
        InlineKeyboardButton("🔙 В профиль", callback_data="profile_menu")
    )
    return keyboard

def get_settings_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура настроек"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить данные", callback_data="settings_edit"),
        InlineKeyboardButton("💳 Мои карты", callback_data="settings_cards")
    )
    keyboard.add(
        InlineKeyboardButton("🔔 Настройки уведомлений", callback_data="settings_notifications"),
        InlineKeyboardButton("🗑️ Удалить аккаунт", callback_data="settings_delete")
    )
    keyboard.add(InlineKeyboardButton("🔙 В профиль", callback_data="profile_menu"))
    return keyboard

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📊 Полная статистика", callback_data="admin_stats"),
        InlineKeyboardButton("📦 Управление заказами", callback_data="admin_orders")
    )
    keyboard.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
        InlineKeyboardButton("💳 Выплаты", callback_data="admin_payouts")
    )
    keyboard.add(
        InlineKeyboardButton("📅 Консультации", callback_data="admin_consultations"),
        InlineKeyboardButton("🖼️ Портфолио", callback_data="admin_portfolio")
    )
    keyboard.add(
        InlineKeyboardButton("✉️ Рассылка", callback_data="admin_mailing"),
        InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    return keyboard

def get_bonus_carousel_keyboard(bonus_id: int, total_bonuses: int) -> InlineKeyboardMarkup:
    """Карусель бонусов"""
    keyboard = InlineKeyboardMarkup()
    
    prev_button = None
    next_button = None
    
    if bonus_id > 1:
        prev_button = InlineKeyboardButton("< Назад", callback_data=f"bonus_{bonus_id-1}")
    
    middle_button = InlineKeyboardButton("📋 Подробнее", callback_data=f"bonus_details_{bonus_id}")
    
    if bonus_id < total_bonuses:
        next_button = InlineKeyboardButton("> Вперёд", callback_data=f"bonus_{bonus_id+1}")
    else:
        next_button = InlineKeyboardButton("🔚", callback_data="bonus_end")
    
    if prev_button and next_button:
        keyboard.row(prev_button, middle_button, next_button)
    elif prev_button:
        keyboard.row(prev_button, middle_button)
    elif next_button:
        keyboard.row(middle_button, next_button)
    else:
        keyboard.row(middle_button)
    
    return keyboard

def get_tracker_keyboard(order_id: int) -> InlineKeyboardMarkup:
    """Клавиатура трекера заказа"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("💬 Чат с менеджером", callback_data=f"tracker_chat_{order_id}"),
        InlineKeyboardButton("📁 Файлы и материалы", callback_data=f"tracker_files_{order_id}")
    )
    keyboard.add(
        InlineKeyboardButton("💳 Оплата", callback_data=f"tracker_payment_{order_id}"),
        InlineKeyboardButton("🔄 Обновить статус", callback_data=f"tracker_refresh_{order_id}")
    )
    keyboard.add(InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    return keyboard

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    args = message.get_args()
    referrer_code = args if args else None
    
    user = await db.get_user(message.from_user.id)
    
    if not user:
        user = await db.create_user(
            message.from_user.id,
            message.from_user.username,
            message.from_user.full_name,
            referrer_code
        )
    
    welcome_text = """🎯 ВАША ЖИЗНЬ — ВАША ИГРА, КОТОРАЯ СТАНЕТ ЛЕГЕНДОЙ!

• Пока другие дарят обычные подарки, которые забываются через неделю...
…вы можете создать личную вселенную. Игру, где зашифрованы ваши шутки, ваши ценности, ваши «а помнишь?..». Это машина времени, понятная только вашему кругу. Самый индивидуальный и долговечный подарок — ваша собственная история, в которую можно играть."""
    
    is_admin = await db.is_admin(user['id'])
    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard(is_admin))

@dp.message_handler(commands=['menu'])
async def cmd_menu(message: types.Message):
    """Обработчик команды /menu"""
    user = await db.get_user(message.from_user.id)
    if not user:
        await cmd_start(message)
        return
    
    menu_text = """📌 ГЛАВНОЕ МЕНЮ:"""
    
    is_admin = await db.is_admin(user['id'])
    await message.answer(menu_text, reply_markup=get_main_menu_keyboard(is_admin))

# ==================== ОБРАБОТЧИКИ КНОПОК ГЛАВНОГО МЕНЮ ====================

@dp.message_handler(lambda message: message.text == "❓ Помощь")
async def help_menu(message: types.Message):
    """Обработчик кнопки Помощь"""
    help_text = """❓ ВЫБЕРИТЕ РАЗДЕЛ:"""
    await message.answer(help_text, reply_markup=get_help_keyboard())

@dp.message_handler(lambda message: message.text == "📞 Контакты")
async def contacts_menu(message: types.Message):
    """Обработчик кнопки Контакты"""
    contacts_text = """📞 КОНТАКТЫ

Свяжитесь с нами:
📱 Телефон: +7 (925) 101-56-63
👨‍💼 Менеджер: @bgh_997
📧 Email: timporsh97@icloud.com
📍 Город: Москва
🕐 Время работы: Пн-Пт 10:00-20:00"""
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu"))
    await message.answer(contacts_text, reply_markup=keyboard)

@dp.message_handler(lambda message: message.text == "👤 Мой профиль")
async def profile_menu(message: types.Message):
    """Обработчик кнопки Мой профиль"""
    user = await db.get_user(message.from_user.id)
    if not user:
        await cmd_start(message)
        return
    
    # Получаем реальную статистику
    user_stats = await db.get_user_statistics(user['id'])
    
    # Формируем текст профиля
    profile_text = f"""👤 ВАШ ПРОФИЛЬ

Личные данные:
👤 Имя: {user.get('full_name', 'Не указано')}
📱 Телефон: {user.get('phone', 'Не указано')}
📧 Email: {user.get('email', 'Не указано')}
📍 Город: {user.get('city', 'Не указано')}
🎉 Дата мероприятия: {user.get('event_date', 'Не указана') if user.get('event_date') else 'Не указана'}

Статистика:
🎮 Всего заказов: {user_stats['user_stats'].get('total_orders_count', 0)}
📦 Номер последнего заказа: #{user_stats['order_history'][0]['order_number'] if user_stats['order_history'] else 'Нет заказов'}
👥 Приглашено друзей: {user_stats['user_stats'].get('referrals_count', 0)}
💎 Накоплено бонусов: {user.get('balance', 0)}₽

Дата регистрации: {user['created_at'].strftime('%d.%m.%Y')}"""
    
    await message.answer(profile_text, reply_markup=get_profile_keyboard())

@dp.message_handler(lambda message: message.text == "🎮 Оформить заявку")
async def order_start(message: types.Message):
    """Обработчик кнопки Оформить заявку"""
    order_text = """🎮 ОФОРМЛЕНИЕ ЗАЯВКИ

Чтобы начать разработку вашей персональной игры, нам нужна базовая информация. Заполнение анкеты займет 2-3 минуты."""
    
    await message.answer(order_text, reply_markup=get_order_start_keyboard())

@dp.message_handler(lambda message: message.text == "🚀 Начать оформление")
async def start_order_creation(message: types.Message):
    """Обработчик кнопки Начать оформление"""
    await OrderForm.step1_name.set()
    await message.answer("""Начинаем создание вашей игры!

Шаг 1/12:
👤 Как вас зовут?
Укажите имя для обращения в работе.""", reply_markup=get_cancel_keyboard())

@dp.message_handler(lambda message: message.text == "👑 Админ")
async def admin_panel(message: types.Message):
    """Обработчик кнопки Админ"""
    user = await db.get_user(message.from_user.id)
    if not user or not await db.is_admin(user['id']):
        return
    
    # Получаем реальную статистику
    stats = await db.get_system_statistics()
    basic_stats = stats['basic']
    
    # Получаем уведомления
    notifications = await db.get_admin_notifications(5)
    
    admin_text = f"""👑 АДМИН ПАНЕЛЬ

Быстрая статистика:
• 👥 Новые пользователи сегодня: {basic_stats.get('new_users_today', 0)}
• 👥 Новые пользователи за неделю: {basic_stats.get('new_users_week', 0)}
• 👥 Новые пользователи за месяц: {basic_stats.get('new_users_month', 0)}
• 📦 Новые заказы сегодня: {basic_stats.get('new_orders_today', 0)}
• 📦 Новые заказы за неделю: {basic_stats.get('new_orders_week', 0)}
• 💰 Выручка сегодня: {basic_stats.get('revenue_today', 0)}₽
• 💰 Выручка за месяц: {basic_stats.get('revenue_month', 0)}₽
• 💰 Выручка за все время: {basic_stats.get('orders_revenue', 0)}₽
• 💬 Консультации сегодня: {basic_stats.get('consultations_today', 0)}
• 💬 Консультации в ближайшее время: {basic_stats.get('consultations_week', 0)}

Требует внимания:
⚠️ Необработанных заявок: {len(notifications)}"""
    
    await message.answer(admin_text, reply_markup=get_admin_keyboard())

# ==================== ОБРАБОТЧИКИ АНКЕТЫ (12 ШАГОВ) ====================

@dp.message_handler(state=OrderForm.step1_name)
async def process_step1_name(message: types.Message, state: FSMContext):
    """Шаг 1: Имя"""
    async with state.proxy() as data:
        data['name'] = message.text
        user = await db.get_user(message.from_user.id)
        data['user_id'] = user['id']
    
    await OrderForm.next()
    await message.answer("""2/12 📞 Ваш контактный телефон для связи?
В формате +7XXX...""", reply_markup=get_cancel_keyboard())

@dp.message_handler(state=OrderForm.step2_phone)
async def process_step2_phone(message: types.Message, state: FSMContext):
    """Шаг 2: Телефон"""
    async with state.proxy() as data:
        data['phone'] = message.text
    
    await OrderForm.next()
    await message.answer("""3/12 📅 Для какого события или даты создаётся игра?
Например: «Юбилей 15.08.2024»""", reply_markup=get_cancel_keyboard())

@dp.message_handler(state=OrderForm.step3_date)
async def process_step3_date(message: types.Message, state: FSMContext):
    """Шаг 3: Дата события"""
    async with state.proxy() as data:
        data['occasion'] = message.text
    
    await OrderForm.next()
    await message.answer("""4/12 🎁 Для кого предназначена игра?
(Выберите или укажите своё)""", reply_markup=get_target_audience_keyboard())

# Шаг 4 - выбор целевой аудитории (inline кнопки)
@dp.callback_query_handler(lambda c: c.data.startswith('target_'), state=OrderForm.step4_target)
async def process_step4_target(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 4: Целевая аудитория"""
    target_map = {
        'target_family': 'Для семьи',
        'target_couple': 'Для второй половинки',
        'target_team': 'Для команды / Коллег',
        'target_friend': 'Для друга',
        'target_other': 'Другое'
    }
    
    async with state.proxy() as data:
        data['target_audience'] = target_map.get(callback_query.data, 'Другое')
    
    await OrderForm.next()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="""5/12 💰 Каков ваш ориентировочный бюджет?""",
        reply_markup=get_budget_keyboard()
    )

# Шаг 5 - бюджет (inline кнопки)
@dp.callback_query_handler(lambda c: c.data.startswith('budget_'), state=OrderForm.step5_budget)
async def process_step5_budget(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 5: Бюджет"""
    budget_map = {
        'budget_5000': 'До 5.000₽',
        'budget_10000': 'До 10.000₽',
        'budget_20000': 'До 20.000₽',
        'budget_20000plus': '+20.000₽',
        'budget_other': 'Другое'
    }
    
    async with state.proxy() as data:
        data['budget'] = budget_map.get(callback_query.data, 'Другое')
    
    await OrderForm.next()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="""6/12 🔢 Сколько игроков будет играть одновременно?""",
        reply_markup=get_players_keyboard()
    )

# Шаг 6 - количество игроков (inline кнопки)
@dp.callback_query_handler(lambda c: c.data.startswith('players_'), state=OrderForm.step6_players)
async def process_step6_players(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 6: Количество игроков"""
    players_map = {
        'players_2_6': '2-6 игроков',
        'players_6_12': '6-12 игроков',
        'players_12plus': '12+ игроков',
        'players_other': 'Другое'
    }
    
    async with state.proxy() as data:
        data['players_count'] = players_map.get(callback_query.data, 'Другое')
        data['emotions'] = []  # Инициализируем список эмоций
    
    await OrderForm.next()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="""7/12 ❤️ Какие эмоции должна вызывать игра? (можно выбрать несколько)""",
        reply_markup=get_emotions_keyboard()
    )

# Шаг 7 - эмоции (множественный выбор)
@dp.callback_query_handler(lambda c: c.data.startswith('emotion_'), state=OrderForm.step7_emotions)
async def process_step7_emotions(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 7: Эмоции (множественный выбор)"""
    emotion_map = {
        'emotion_fun': 'Веселье и смех',
        'emotion_warmth': 'Тепло и ностальгия',
        'emotion_excitement': 'Азарт и соперничество',
        'emotion_strategy': 'Стратегия и мысли',
        'emotion_team': 'Командный дух',
        'emotion_other': 'Другое'
    }
    
    async with state.proxy() as data:
        emotions = data.get('emotions', [])
        
        if callback_query.data == 'emotions_next':
            # Проверяем, что выбрана хотя бы одна эмоция
            if not emotions:
                await bot.answer_callback_query(callback_query.id, "Пожалуйста, выберите хотя бы одну эмоцию")
                return
            
            # Переход к следующему шагу
            await OrderForm.next()
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text="""8/12 🎯 На основе какой игры вы хотите создать свою?
Например: «Монополия», «Алиас», «Крокодил» или своя уникальная механика.""",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        emotion = emotion_map.get(callback_query.data)
        if emotion:
            if emotion in emotions:
                emotions.remove(emotion)  # Снимаем выбор
            else:
                emotions.append(emotion)  # Добавляем выбор
            
            data['emotions'] = emotions
            
            # Обновляем сообщение с текущим выбором
            selected = ', '.join(emotions) if emotions else 'Не выбрано'
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=f"""7/12 ❤️ Какие эмоции должна вызывать игра? (можно выбрать несколько)

Выбрано: {selected}""",
                reply_markup=get_emotions_keyboard()
            )

@dp.message_handler(state=OrderForm.step8_basis)
async def process_step8_basis(message: types.Message, state: FSMContext):
    """Шаг 8: Основа игры"""
    async with state.proxy() as data:
        data['game_basis'] = message.text
    
    await OrderForm.next()
    await message.answer("""9/12 🌟 Как вы о нас узнали?""", reply_markup=get_source_keyboard())

# Шаг 9 - источник (inline кнопки)
@dp.callback_query_handler(lambda c: c.data.startswith('source_'), state=OrderForm.step9_source)
async def process_step9_source(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 9: Источник"""
    source_map = {
        'source_social': 'Соцсети',
        'source_referral': 'Реферальная система',
        'source_friends': 'Рекомендация друзей',
        'source_telegram': 'Реклама в Telegram',
        'source_other': 'Другое'
    }
    
    async with state.proxy() as data:
        data['source'] = source_map.get(callback_query.data, 'Другое')
    
    await OrderForm.next()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="""10/12 🕕 Как часто вы играете в настольные игры?""",
        reply_markup=get_frequency_keyboard()
    )

# Шаг 10 - частота игры (inline кнопки)
@dp.callback_query_handler(lambda c: c.data.startswith('frequency_'), state=OrderForm.step10_frequency)
async def process_step10_frequency(callback_query: types.CallbackQuery, state: FSMContext):
    """Шаг 10: Частота игры"""
    frequency_map = {
        'frequency_never': 'Не играю, но хочу начать',
        'frequency_rare': 'Редко, по особым случаям',
        'frequency_regular': 'Регулярно, это семейная традиция',
        'frequency_often': 'Часто, я настоящий профессионал!',
        'frequency_other': 'Другое'
    }
    
    async with state.proxy() as data:
        data['play_frequency'] = frequency_map.get(callback_query.data, 'Другое')
    
    await OrderForm.next()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="""11/12 📝 Опишите игру одним предложением.
«Это игра о нашем семейном путешествии в Грузию с весёлыми заданиями».""",
        reply_markup=get_cancel_keyboard()
    )

# Шаг 11 - описание
@dp.message_handler(state=OrderForm.step11_description)
async def process_step11_description(message: types.Message, state: FSMContext):
    """Шаг 11: Описание игры"""
    async with state.proxy() as data:
        data['description'] = message.text
        data['game_name'] = message.text[:100]  # Используем описание как название
    
    await OrderForm.next()
    await message.answer("""12/12 📲 Напишите ваш Telegram-ник для быстрой связи
@username""", reply_markup=get_cancel_keyboard())

# Шаг 12 - Telegram username
@dp.message_handler(state=OrderForm.step12_telegram)
async def process_step12_telegram(message: types.Message, state: FSMContext):
    """Шаг 12: Telegram username"""
    user = await db.get_user(message.from_user.id)
    
    async with state.proxy() as data:
        data['telegram_username'] = message.text
        
        # Сохраняем заказ в БД
        order_data = {
            'game_name': data.get('game_name'),
            'occasion': data.get('occasion'),
            'target_audience': data.get('target_audience'),
            'budget': data.get('budget'),
            'players_count': data.get('players_count'),
            'emotions': data.get('emotions', []),
            'game_basis': data.get('game_basis'),
            'source': data.get('source'),
            'play_frequency': data.get('play_frequency'),
            'description': data.get('description'),
            'telegram_username': data.get('telegram_username')
        }
        
        order = await db.create_order(user['id'], order_data)
    
    # Отправляем финальное сообщение
    complete_text = """✅ Анкета заполнена! Спасибо за ваши ответы!

🎯 Что дальше:
Я изучу ваши ответы и в течение 24 часов с вами свяжется менеджер для обсуждения концепции и детального расчёта!"""
    
    await message.answer(complete_text, reply_markup=get_order_complete_keyboard())
    await state.finish()

# ==================== ОБРАБОТЧИКИ INLINE КНОПОК ====================

@dp.callback_query_handler(lambda c: c.data == 'main_menu')
async def process_main_menu(callback_query: types.CallbackQuery):
    """Обработчик кнопки Главное меню"""
    user = await db.get_user(callback_query.from_user.id)
    is_admin = await db.is_admin(user['id']) if user else False
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="📌 ГЛАВНОЕ МЕНЮ:",
        reply_markup=get_main_menu_keyboard(is_admin)
    )

@dp.callback_query_handler(lambda c: c.data == 'back_to_help')
async def process_back_to_help(callback_query: types.CallbackQuery):
    """Обработчик кнопки Назад в помощь"""
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="❓ ВЫБЕРИТЕ РАЗДЕЛ:",
        reply_markup=get_help_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == 'help_order')
async def process_help_order(callback_query: types.CallbackQuery):
    """Обработчик раздела Создание заказа в помощи"""
    help_order_text = """🎮 СОЗДАНИЕ ЗАКАЗА

1. Нажмите кнопку "Оформить заявку" в главном меню
2. Заполните анкету из 12 вопросов о вашей игре
3. Наш менеджер свяжется с вами в течение 24 часов
4. Если возникнут трудности при оформлении, обратитесь к менеджеру @bgh_997 для персональной помощи"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=help_order_text,
        reply_markup=get_back_to_help_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == 'help_consultation')
async def process_help_consultation(callback_query: types.CallbackQuery):
    """Обработчик раздела Консультация в помощи"""
    help_consultation_text = """💬 КОНСУЛЬТАЦИЯ

📅 Хотите обсудить свою игру? Забронируйте консультацию!

На 45-минутной встрече мы ответим на все ваши вопросы, поможем сформулировать идею и покажем, как мы сможем воплотить её в жизнь от эскиза до готовой коробки.

Детали:
• Длительность: 45 минут
• Стоимость: 450 рублей
• Приятный бонус: Если после консультации вы решите заказать игру, её стоимость будет ниже на 5%"""
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📅 Выбрать дату и время", callback_data="book_consultation_start"),
        InlineKeyboardButton("❓ Частые вопросы", callback_data="consultation_faq")
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_help"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=help_consultation_text,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data == 'book_consultation_start')
async def start_booking_consultation(callback_query: types.CallbackQuery):
    """Начало бронирования консультации"""
    await ConsultationForm.choose_date.set()
    
    # Получаем доступные даты из БД
    slots = await db.get_available_slots()
    
    if not slots:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="На данный момент нет доступных слотов для консультаций.\nПожалуйста, обратитесь к менеджеру @bgh_997",
            reply_markup=get_back_to_help_keyboard()
        )
        await ConsultationForm.choose_date.finish()
        return
    
    # Группируем слоты по датам
    dates = {}
    for slot in slots:
        date_str = slot['slot_date'].strftime('%d.%m.%Y')
        if date_str not in dates:
            dates[date_str] = []
        dates[date_str].append(slot)
    
    # Формируем клавиатуру с датами
    keyboard = InlineKeyboardMarkup(row_width=2)
    buttons = []
    for date_str, date_slots in list(dates.items())[:8]:  # Ограничиваем 8 датами
        buttons.append(InlineKeyboardButton(
            f"📅 {date_str} ({len(date_slots)} слотов)",
            callback_data=f"consult_date_{date_str}"
        ))
    
    # Добавляем кнопки построчно
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            keyboard.row(buttons[i], buttons[i+1])
        else:
            keyboard.row(buttons[i])
    
    keyboard.add(InlineKeyboardButton("🔄 Обновить доступные даты", callback_data="book_consultation_start"))
    keyboard.add(InlineKeyboardButton("❓ Нет подходящей даты?", callback_data="consultation_no_date"))
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_help"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="📅 ВЫБОР ДАТЫ КОНСУЛЬТАЦИИ\n\nВыберите удобную дату из доступных:",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('consult_date_'), state=ConsultationForm.choose_date)
async def choose_consultation_date(callback_query: types.CallbackQuery, state: FSMContext):
    """Выбор даты консультации"""
    date_str = callback_query.data.replace('consult_date_', '')
    
    async with state.proxy() as data:
        data['consultation_date'] = date_str
    
    # Получаем слоты для выбранной даты
    try:
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        slots = await db.get_slots_by_date(date_obj.strftime('%Y-%m-%d'))
    except:
        slots = []
    
    if not slots:
        await bot.answer_callback_query(callback_query.id, "На выбранную дату нет доступных слотов")
        return
    
    await ConsultationForm.next()
    
    # Формируем клавиатуру со временем
    keyboard = InlineKeyboardMarkup(row_width=3)
    buttons = []
    for slot in slots:
        time_str = slot['slot_time'].strftime('%H:%M')
        buttons.append(InlineKeyboardButton(
            f"🕐 {time_str}",
            callback_data=f"consult_time_{slot['id']}"
        ))
    
    # Добавляем кнопки построчно
    for i in range(0, len(buttons), 3):
        row_buttons = buttons[i:i+3]
        keyboard.row(*row_buttons)
    
    keyboard.add(InlineKeyboardButton("📅 Выбрать другую дату", callback_data="book_consultation_start"))
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_help"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"📅 {date_str}\n\nВыберите удобное время:",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('consult_time_'), state=ConsultationForm.choose_time)
async def choose_consultation_time(callback_query: types.CallbackQuery, state: FSMContext):
    """Выбор времени консультации"""
    slot_id = int(callback_query.data.replace('consult_time_', ''))
    
    async with state.proxy() as data:
        data['slot_id'] = slot_id
    
    await ConsultationForm.next()
    
    # Получаем информацию о слоте
    slots = await db.get_available_slots()
    slot_info = next((s for s in slots if s['id'] == slot_id), None)
    
    if not slot_info:
        await bot.answer_callback_query(callback_query.id, "Слот больше не доступен")
        return
    
    consultation_text = f"""✅ ВРЕМЯ ВЫБРАНО!

Детали бронирования:
📅 Дата: {slot_info['slot_date'].strftime('%d.%m.%Y')}
🕐 Время: {slot_info['slot_time'].strftime('%H:%M')} - {(datetime.strptime(slot_info['slot_time'].strftime('%H:%M'), '%H:%M') + timedelta(minutes=45)).strftime('%H:%M')}
⏱️ Длительность: 45 минут
💰 Стоимость: 450₽

Для подтверждения записи необходимо оплатить консультацию.

Реквизиты для оплаты:
🏦 Банк: Тинькофф
💳 Номер карты: 2200 **** **** 5678
👤 Получатель: Тимофей

После оплаты:

1. Сделайте скриншот чека
2. Отправьте его менеджеру @bgh_997
3. Мы подтвердим вашу запись в течение 1 часа"""
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("💳 Оплатить 450₽", callback_data="confirm_consultation_payment"),
        InlineKeyboardButton("✏️ Изменить время", callback_data="book_consultation_start")
    )
    keyboard.add(
        InlineKeyboardButton("📅 Выбрать другую дату", callback_data="book_consultation_start"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel_consultation")
    )
    keyboard.add(InlineKeyboardButton("💬 Связаться с менеджером", callback_data="contact_manager"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=consultation_text,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data == 'confirm_consultation_payment', state=ConsultationForm.payment)
async def confirm_consultation_payment(callback_query: types.CallbackQuery, state: FSMContext):
    """Подтверждение оплаты консультации"""
    user = await db.get_user(callback_query.from_user.id)
    
    async with state.proxy() as data:
        slot_id = data.get('slot_id')
    
    # Бронируем консультацию
    consultation = await db.book_consultation(user['id'], slot_id)
    
    if not consultation:
        await bot.answer_callback_query(callback_query.id, "Ошибка бронирования. Слот уже занят.")
        return
    
    consultation_text = """🔄 ОЖИДАНИЕ ПОДТВЕРЖДЕНИЯ

Ваша запись ожидает подтверждения оплаты.

Что дальше:

1. Отправьте скриншот чека менеджеру @bgh_997
2. Мы проверим оплату в течение 1 часа
3. Вы получите подтверждение записи
4. За сутки до консультации придёт напоминание

Детали записи:
📅 Дата будет указана после подтверждения
💰 450₽
⏱️ 45 минут"""
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📤 Отправить скриншот", callback_data="send_receipt"),
        InlineKeyboardButton("✏️ Изменить запись", callback_data="book_consultation_start")
    )
    keyboard.add(
        InlineKeyboardButton("❌ Отменить запись", callback_data="cancel_consultation"),
        InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=consultation_text,
        reply_markup=keyboard
    )
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'cancel_order', state='*')
async def cancel_order(callback_query: types.CallbackQuery, state: FSMContext):
    """Отмена создания заказа"""
    await state.finish()
    user = await db.get_user(callback_query.from_user.id)
    is_admin = await db.is_admin(user['id']) if user else False
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Создание заказа отменено.",
        reply_markup=None
    )
    
    await bot.send_message(
        callback_query.message.chat.id,
        "📌 ГЛАВНОЕ МЕНЮ:",
        reply_markup=get_main_menu_keyboard(is_admin)
    )

@dp.callback_query_handler(lambda c: c.data == 'cancel_consultation', state='*')
async def cancel_consultation(callback_query: types.CallbackQuery, state: FSMContext):
    """Отмена бронирования консультации"""
    await state.finish()
    user = await db.get_user(callback_query.from_user.id)
    is_admin = await db.is_admin(user['id']) if user else False
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Бронирование консультации отменено.",
        reply_markup=None
    )
    
    await bot.send_message(
        callback_query.message.chat.id,
        "📌 ГЛАВНОЕ МЕНЮ:",
        reply_markup=get_main_menu_keyboard(is_admin)
    )

# ==================== ОБРАБОТЧИКИ ПРОФИЛЯ ====================

@dp.callback_query_handler(lambda c: c.data == 'profile_menu')
async def process_profile_menu(callback_query: types.CallbackQuery):
    """Обработчик меню профиля"""
    user = await db.get_user(callback_query.from_user.id)
    
    # Получаем реальную статистику
    user_stats = await db.get_user_statistics(user['id'])
    
    # Формируем текст профиля
    profile_text = f"""👤 ВАШ ПРОФИЛЬ

Личные данные:
👤 Имя: {user.get('full_name', 'Не указано')}
📱 Телефон: {user.get('phone', 'Не указано')}
📧 Email: {user.get('email', 'Не указано')}
📍 Город: {user.get('city', 'Не указано')}
🎉 Дата мероприятия: {user.get('event_date', 'Не указана') if user.get('event_date') else 'Не указана'}

Статистика:
🎮 Всего заказов: {user_stats['user_stats'].get('total_orders_count', 0)}
📦 Номер последнего заказа: #{user_stats['order_history'][0]['order_number'] if user_stats['order_history'] else 'Нет заказов'}
👥 Приглашено друзей: {user_stats['user_stats'].get('referrals_count', 0)}
💎 Накоплено бонусов: {user.get('balance', 0)}₽

Дата регистрации: {user['created_at'].strftime('%d.%m.%Y')}"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=profile_text,
        reply_markup=get_profile_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == 'profile_balance')
async def process_profile_balance(callback_query: types.CallbackQuery):
    """Обработчик баланса"""
    user = await db.get_user(callback_query.from_user.id)
    
    balance_text = f"""💰 ВАШ БАЛАНС

Текущий баланс: {user.get('balance', 0)}₽
Минимальный вывод: 2 000₽
Комиссия: 0%
Срок обработки: 1-3 рабочих дня"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=balance_text,
        reply_markup=get_balance_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == 'balance_withdraw')
async def process_balance_withdraw(callback_query: types.CallbackQuery):
    """Обработчик вывода средств"""
    await PayoutForm.enter_amount.set()
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="Введите сумму для вывода (мин. 2000₽):",
        reply_markup=None
    )

@dp.message_handler(state=PayoutForm.enter_amount)
async def process_payout_amount(message: types.Message, state: FSMContext):
    """Обработка суммы вывода"""
    try:
        amount = int(message.text)
        if amount < 2000:
            await message.answer("Минимальная сумма вывода 2000₽. Введите сумму:")
            return
    except:
        await message.answer("Пожалуйста, введите число:")
        return
    
    async with state.proxy() as data:
        data['amount'] = amount
    
    await PayoutForm.next()
    await message.answer("Введите номер карты (формат: 2200 1234 5678 9010):")

@dp.message_handler(state=PayoutForm.enter_card)
async def process_payout_card(message: types.Message, state: FSMContext):
    """Обработка номера карты"""
    card_number = ''.join(filter(str.isdigit, message.text))
    if len(card_number) != 16:
        await message.answer("Номер карты должен содержать 16 цифр. Введите еще раз:")
        return
    
    async with state.proxy() as data:
        data['card_number'] = f"**** {card_number[-4:]}"
    
    await PayoutForm.next()
    await message.answer("Введите имя владельца карты:")

@dp.message_handler(state=PayoutForm.enter_card_holder)
async def process_payout_card_holder(message: types.Message, state: FSMContext):
    """Обработка имени владельца карты"""
    user = await db.get_user(message.from_user.id)
    
    async with state.proxy() as data:
        data['card_holder'] = message.text
        
        # Создаем заявку на вывод
        payout = await db.create_payout_request(
            user['id'],
            data['amount'],
            data['card_number'],
            data['card_holder']
        )
    
    if payout:
        await message.answer(f"Заявка на вывод {data['amount']}₽ создана. Ожидайте обработки.")
    else:
        await message.answer("Ошибка создания заявки. Проверьте баланс.")
    
    await state.finish()
    
    # Возвращаем в меню баланса
    balance_text = f"""💰 ВАШ БАЛАНС

Текущий баланс: {user.get('balance', 0)}₽
Минимальный вывод: 2 000₽
Комиссия: 0%
Срок обработки: 1-3 рабочих дня"""
    
    await message.answer(balance_text, reply_markup=get_balance_keyboard())

@dp.callback_query_handler(lambda c: c.data == 'profile_referrals')
async def process_profile_referrals(callback_query: types.CallbackQuery):
    """Обработчик рефералов"""
    user = await db.get_user(callback_query.from_user.id)
    
    # Получаем реальную статистику
    referral_stats = await db.get_user_referral_stats(user['id'])
    
    # Формируем реферальную ссылку
    ref_link = f"https://t.me/storygame_bot?start=ref_{user['referral_code']}"
    
    referrals_text = f"""👥 РЕФЕРАЛЬНАЯ СИСТЕМА

Ваша ссылка для приглашений:
{ref_link}

Статистика:
👥 Приглашено клиентов: {referral_stats.get('total_referrals', 0)}
💰 Заработано на рефералах: {referral_stats.get('referral_revenue', 0)}₽
⏳ Ожидает выплаты: {referral_stats.get('pending_earnings', 0)}₽

Как это работает:

1. Клиент переходит по вашей ссылке
2. Вы получаете +400₽ на баланс (после оплаты заказа)
3. Дополнительно вы получаете 10% от всех его заказов"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=referrals_text,
        reply_markup=get_referrals_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == 'profile_stats')
async def process_profile_stats(callback_query: types.CallbackQuery):
    """Обработчик статистики профиля"""
    user = await db.get_user(callback_query.from_user.id)
    
    # Получаем реальную статистику
    user_stats = await db.get_user_statistics(user['id'])
    stats = user_stats['user_stats']
    
    # Получаем активный заказ
    orders = await db.get_user_orders(user['id'], 1)
    active_order = orders[0] if orders and orders[0].get('status') == 'active' else None
    
    stats_text = f"""📊 ВАША СТАТИСТИКА

Общая информация:
🎮 Всего заказов: {stats.get('total_orders_count', 0)}
📦 Активный заказ: #{active_order['order_number'] if active_order else 'Нет активных'}
👥 Приглашено клиентов: {stats.get('referrals_count', 0)}
💎 Накоплено бонусов: {user.get('balance', 0)}₽

История активности:
📅 {user['created_at'].strftime('%d.%m.%Y')} - Регистрация в боте"""
    
    # Добавляем историю заказов
    for i, order in enumerate(user_stats['order_history'][:3]):
        stats_text += f"\n🎮 {order['created_at'].strftime('%d.%m.%Y')} - Создан заказ #{order['order_number']}"
    
    if stats.get('referrals_count', 0) > 0:
        stats_text += f"\n👥 {datetime.now().strftime('%d.%m.%Y')} - Приглашён первый клиент"
    
    if user.get('balance', 0) > 0:
        stats_text += f"\n💰 {datetime.now().strftime('%d.%m.%Y')} - Получен бонус {min(user.get('balance', 0), 400)}₽"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📈 График активности", callback_data="stats_graph"),
        InlineKeyboardButton("🎮 История заказов", callback_data="stats_orders")
    )
    keyboard.add(
        InlineKeyboardButton("👥 Реферальная статистика", callback_data="stats_referrals"),
        InlineKeyboardButton("🔙 В профиль", callback_data="profile_menu")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=stats_text,
        reply_markup=keyboard
    )

# ==================== ОБРАБОТЧИКИ БОНУСОВ ====================

@dp.callback_query_handler(lambda c: c.data == 'balance_bonuses')
async def process_balance_bonuses(callback_query: types.CallbackQuery):
    """Обработчик бонусов"""
    bonuses = await db.get_bonuses()
    
    if not bonuses:
        await bot.answer_callback_query(callback_query.id, "Бонусы временно недоступны")
        return
    
    # Показываем первый бонус
    bonus = bonuses[0]
    bonuses_text = f"""🎁 Бонусная программа «Создатели Легенд»

Станьте нашим амбассадором и получайте вознаграждение, помогая находить тех, чьи истории достойны стать играми. Каждый бонус — это отдельная задача с чёткими правилами. Вместе мы создадим больше легенд!

---

{bonus['icon']} Бонус 1/{len(bonuses)}: «{bonus['name']}»

Суть: {bonus['description']}
Награда: {bonus['reward']}₽"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=bonuses_text,
        reply_markup=get_bonus_carousel_keyboard(1, len(bonuses))
    )

@dp.callback_query_handler(lambda c: c.data.startswith('bonus_'))
async def process_bonus_carousel(callback_query: types.CallbackQuery):
    """Обработчик карусели бонусов"""
    try:
        bonus_id = int(callback_query.data.replace('bonus_', ''))
    except:
        bonus_id = 1
    
    bonuses = await db.get_bonuses()
    
    if not bonuses or bonus_id < 1 or bonus_id > len(bonuses):
        await bot.answer_callback_query(callback_query.id, "Бонус не найден")
        return
    
    bonus = bonuses[bonus_id - 1]
    bonuses_text = f"""🎁 Бонусная программа «Создатели Легенд»

Станьте нашим амбассадором и получайте вознаграждение, помогая находить тех, чьи истории достойны стать играми. Каждый бонус — это отдельная задача с чёткими правилами. Вместе мы создадим больше легенд!

---

{bonus['icon']} Бонус {bonus_id}/{len(bonuses)}: «{bonus['name']}»

Суть: {bonus['description']}
Награда: {bonus['reward']}₽"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=bonuses_text,
        reply_markup=get_bonus_carousel_keyboard(bonus_id, len(bonuses))
    )

@dp.callback_query_handler(lambda c: c.data.startswith('bonus_details_'))
async def process_bonus_details(callback_query: types.CallbackQuery):
    """Обработчик деталей бонуса"""
    try:
        bonus_id = int(callback_query.data.replace('bonus_details_', ''))
    except:
        bonus_id = 1
    
    bonus = await db.get_bonus(bonus_id)
    
    if not bonus:
        await bot.answer_callback_query(callback_query.id, "Бонус не найден")
        return
    
    bonus_details = bonus.get('detailed_description', 'Описание временно недоступно')
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ АКТИВИРОВАТЬ БОНУС", callback_data=f"activate_bonus_{bonus_id}"),
        InlineKeyboardButton("📞 Обсудить с менеджером", url="https://t.me/bgh_997")
    )
    keyboard.add(InlineKeyboardButton("🔙 К списку бонусов", callback_data="balance_bonuses"))
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=bonus_details,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('activate_bonus_'))
async def process_activate_bonus(callback_query: types.CallbackQuery):
    """Обработчик активации бонуса"""
    try:
        bonus_id = int(callback_query.data.replace('activate_bonus_', ''))
    except:
        bonus_id = 1
    
    user = await db.get_user(callback_query.from_user.id)
    
    # Активируем бонус
    user_bonus = await db.activate_bonus(user['id'], bonus_id)
    
    if not user_bonus:
        activation_text = """⚠️ НЕВОЗМОЖНО АКТИВИРОВАТЬ БОНУС

У вас уже активировано максимальное количество бонусов (2/2).

Завершите один из активных бонусов, чтобы активировать новый."""
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔙 К списку бонусов", callback_data="balance_bonuses"))
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=activation_text,
            reply_markup=keyboard
        )
        return
    
    bonus = await db.get_bonus(bonus_id)
    
    activation_text = f"""⚠️ ПОДТВЕРЖДЕНИЕ АКТИВАЦИИ

Вы активируете бонус: «{bonus['name']}»

Важно:

1. Вы можете активировать максимум 2 бонуса одновременно
2. После активации начнётся отсчёт срока выполнения
3. В случае неудачи бонус будет недоступен 30 дней
4. Все условия должны быть выполнены строго

Текущие активные бонусы: 1/2"""
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("✅ ПОДТВЕРДИТЬ АКТИВАЦИЮ", callback_data=f"confirm_activate_bonus_{bonus_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"bonus_details_{bonus_id}"),
        InlineKeyboardButton("📞 Задать вопрос", url="https://t.me/bgh_997")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=activation_text,
        reply_markup=keyboard
    )

# ==================== ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ ====================

@dp.callback_query_handler(lambda c: c.data == 'admin_stats')
async def process_admin_stats(callback_query: types.CallbackQuery):
    """Обработчик полной статистики админа"""
    user = await db.get_user(callback_query.from_user.id)
    if not user or not await db.is_admin(user['id']):
        return
    
    # Получаем реальную статистику
    stats = await db.get_system_statistics()
    basic_stats = stats['basic']
    
    stats_text = f"""📊 ПОЛНАЯ СТАТИСТИКА СИСТЕМЫ

Пользователи:
• 👥 Всего пользователей: {basic_stats.get('total_users', 0)}
• 👥 Новые сегодня: {basic_stats.get('new_users_today', 0)}
• 👥 Новые за неделю: {basic_stats.get('new_users_week', 0)}
• 👥 Новые за месяц: {basic_stats.get('new_users_month', 0)}
• 👥 Активных пользователей: {basic_stats.get('active_users_week', 0)} (за неделю)
• 👥 Рефереров: {basic_stats.get('referrers_count', 0)}

Заказы:
• 📦 Всего заказов: {basic_stats.get('total_orders', 0)}
• 📦 Новые сегодня: {basic_stats.get('new_orders_today', 0)}
• 📦 Новые за неделю: {basic_stats.get('new_orders_week', 0)}
• 📦 Активные заказы: {basic_stats.get('active_orders', 0)}
• 📦 Завершённые заказы: {basic_stats.get('completed_orders', 0)}
• 📦 Средний чек: {basic_stats.get('avg_order_price', 0)}₽

Финансы:
• 💰 Выручка сегодня: {basic_stats.get('revenue_today', 0)}₽
• 💰 Выручка за месяц: {basic_stats.get('revenue_month', 0)}₽
• 💰 Выручка за все время: {basic_stats.get('orders_revenue', 0)}₽
• 💰 Выплачено бонусов: {basic_stats.get('bonuses_paid', 0)}₽
• 💰 Ожидают выплаты: {basic_stats.get('pending_payouts_amount', 0)}₽

Консультации:
• 💬 Всего консультаций: {basic_stats.get('total_consultations', 0)}
• 💬 Консультации сегодня: {basic_stats.get('consultations_today', 0)}
• 💬 Консультации в ближайшее время: {basic_stats.get('consultations_week', 0)}
• 💬 Средняя оценка: 4.7/5
• 💬 Конверсия в заказ: {stats.get('conversion_rate', 0)}%"""
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📈 Графики", callback_data="admin_stats_graphs"),
        InlineKeyboardButton("👥 Топ пользователи", callback_data="admin_top_users")
    )
    keyboard.add(
        InlineKeyboardButton("📦 Топ по заказам", callback_data="admin_top_orders"),
        InlineKeyboardButton("💰 Топ по выплатам", callback_data="admin_top_payouts")
    )
    keyboard.add(
        InlineKeyboardButton("📤 Экспорт в Excel", callback_data="admin_export"),
        InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel_back")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=stats_text,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data == 'admin_panel_back')
async def process_admin_panel_back(callback_query: types.CallbackQuery):
    """Обработчик возврата в админ-панель"""
    user = await db.get_user(callback_query.from_user.id)
    if not user or not await db.is_admin(user['id']):
        return
    
    # Получаем реальную статистику
    stats = await db.get_system_statistics()
    basic_stats = stats['basic']
    
    # Получаем уведомления
    notifications = await db.get_admin_notifications(5)
    
    admin_text = f"""👑 АДМИН ПАНЕЛЬ

Быстрая статистика:
• 👥 Новые пользователи сегодня: {basic_stats.get('new_users_today', 0)}
• 👥 Новые пользователи за неделю: {basic_stats.get('new_users_week', 0)}
• 👥 Новые пользователи за месяц: {basic_stats.get('new_users_month', 0)}
• 📦 Новые заказы сегодня: {basic_stats.get('new_orders_today', 0)}
• 📦 Новые заказы за неделю: {basic_stats.get('new_orders_week', 0)}
• 💰 Выручка сегодня: {basic_stats.get('revenue_today', 0)}₽
• 💰 Выручка за месяц: {basic_stats.get('revenue_month', 0)}₽
• 💰 Выручка за все время: {basic_stats.get('orders_revenue', 0)}₽
• 💬 Консультации сегодня: {basic_stats.get('consultations_today', 0)}
• 💬 Консультации в ближайшее время: {basic_stats.get('consultations_week', 0)}

Требует внимания:
⚠️ Необработанных заявок: {len(notifications)}"""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=admin_text,
        reply_markup=get_admin_keyboard()
    )

# ==================== ТРЕКЕР ЗАКАЗА ====================

@dp.callback_query_handler(lambda c: c.data.startswith('tracker_'))
async def process_tracker(callback_query: types.CallbackQuery):
    """Обработчик трекера заказа"""
    try:
        if callback_query.data.startswith('tracker_chat_'):
            order_id = int(callback_query.data.replace('tracker_chat_', ''))
            await bot.answer_callback_query(callback_query.id, "Связь с менеджером: @bgh_997")
            
        elif callback_query.data.startswith('tracker_refresh_'):
            order_id = int(callback_query.data.replace('tracker_refresh_', ''))
            await show_order_tracker(callback_query, order_id)
            
        elif callback_query.data.startswith('tracker_'):
            order_id = int(callback_query.data.split('_')[1])
            await show_order_tracker(callback_query, order_id)
            
    except Exception as e:
        logger.error(f"Ошибка обработки трекера: {e}")
        await bot.answer_callback_query(callback_query.id, "Ошибка загрузки трекера")

async def show_order_tracker(callback_query: types.CallbackQuery, order_id: int):
    """Показать трекер заказа"""
    tracker = await db.get_order_tracker(order_id)
    
    if not tracker or 'order' not in tracker:
        await bot.answer_callback_query(callback_query.id, "Заказ не найден")
        return
    
    order = tracker['order']
    stages = tracker['stages']
    progress = tracker['progress_percent']
    
    # Формируем прогресс-бар
    progress_bar_length = 20
    filled = int(progress * progress_bar_length / 100)
    progress_bar = "█" * filled + "░" * (progress_bar_length - filled)
    
    tracker_text = f"""🚚 ТРЕКЕР ЗАКАЗА #{order['order_number']}

🎮 "{order['game_name'] or 'Название игры'}"
📅 Срок выполнения: до {order['deadline'].strftime('%d.%m.%Y') if order['deadline'] else 'не установлен'}
💰 Стоимость: {order['price'] or 0}₽
👤 Ответственный: {order['manager_id'] or 'Не назначен'}

━━━━━━━━━━━━━━━━━━━━
📊 ПРОГРЕСС: {order['current_stage']}/{order['total_stages']} этапов
[{progress_bar}] {progress}%

━━━━━━━━━━━━━━━━━━━━
📋 ЭТАПЫ ВЫПОЛНЕНИЯ:"""
    
    for stage in stages:
        status = "✅" if stage['completed'] else "🔄" if stage['stage_number'] == order['current_stage'] else "⏳"
        tracker_text += f"\n\n{status} {stage['stage_number']}. {stage['stage_name']}"
        
        if stage['start_date']:
            tracker_text += f"\n📅 {stage['start_date'].strftime('%d.%m.%Y')} - {stage['end_date'].strftime('%d.%m.%Y') if stage['end_date'] else '...'}"
        
        if stage['completed'] and stage['completed_at']:
            tracker_text += f"\n✓ {stage['description'] or 'Завершено'}"
        elif stage['stage_number'] == order['current_stage']:
            tracker_text += f"\n⏳ В работе"
        else:
            tracker_text += f"\n📌 Ожидает начала"
    
    if tracker['last_manager_comment']:
        tracker_text += f"\n\n━━━━━━━━━━━━━━━━━━━━\n💬 ПОСЛЕДНИЙ КОММЕНТАРИЙ МЕНЕДЖЕРА:\n\"{tracker['last_manager_comment']}\""
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=tracker_text,
        reply_markup=get_tracker_keyboard(order_id)
    )

# ==================== АВТОМАТИЧЕСКИЕ ЗАДАЧИ ====================

async def schedule_tasks():
    """Планировщик автоматических задач"""
    # Ежедневный отчет в 09:00
    aioschedule.every().day.at("09:00").do(db.send_daily_report)
    
    # Проверка незавершенных заявок каждые 6 часов
    aioschedule.every(6).hours.do(db.check_incomplete_orders)
    
    # Напоминания о консультациях каждый день в 10:00
    aioschedule.every().day.at("10:00").do(db.send_consultation_reminders)
    
    # Проверка дедлайнов каждый день в 11:00
    aioschedule.every().day.at("11:00").do(db.check_order_deadlines)
    
    # Обновление статистики каждый час
    aioschedule.every().hour.do(db.get_system_statistics, force_refresh=True)
    
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(60)

# ==================== ЗАПУСК БОТА ====================

async def on_startup(dp):
    """Действия при запуске бота"""
    try:
        # 1. СНАЧАЛА подключаем базу данных
        await db.connect()
        
        # 2. ПОТОМ всё остальное
        asyncio.create_task(start_web_server()) 
        asyncio.create_task(schedule_tasks())
        
        logger.info("Бот запущен и готов к работе")
        
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
        # Не даем боту запуститься, если база не подключена
        import sys
        sys.exit(1)

async def on_shutdown(dp):
    """Действия при выключении бота"""
    await db.close()
    logger.info("Бот выключен")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(start_web_server())
    executor.start_polling(dp, skip_updates=True)