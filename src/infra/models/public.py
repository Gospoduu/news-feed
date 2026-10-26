"""SQLAlchemy Core tables for the datasets shipped with the application."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, UUID


metadata = MetaData(schema="public")


# Some UIDs are present only in the links or payments CSVs.  Keeping them in a
# separate table lets the foreign keys describe every relationship in the
# source data, not only the subset for which a company profile is available.
counterparties = Table(
    "counterparties",
    metadata,
    Column("uid", Text, primary_key=True),
)


# There is one repeated SYNTHETIC_U with distinct records in the source file,
# so a profile is a source row rather than a one-to-one extension of a UID.
company_profiles = Table(
    "company_profiles",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_row", Integer, nullable=False),
    Column(
        "uid",
        Text,
        ForeignKey("counterparties.uid", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("org_type", Text, nullable=False),
    Column("okfs_name", Text, nullable=False),
    Column("company_age_years_gradation", Text, nullable=False),
    Column("eco_mark_label", Text),
    Column("main_okved_code", Text, nullable=False),
    Column("main_okved_name", Text, nullable=False),
    Column("negative_mark", Boolean, nullable=False),
    Column("amount_workers_fns_gradation", Text, nullable=False),
    Column("amount_workers_spark_gradation", Text),
    Column("client_type", Text, nullable=False),
    Column("product_rko_mark", Boolean, nullable=False),
    Column("product_gar_mark", Boolean, nullable=False),
    Column("product_loa_mark", Boolean, nullable=False),
    Column("product_lea_mark", Boolean, nullable=False),
    Column("product_dep_mark", Boolean, nullable=False),
    Column("product_zpp_mark", Boolean, nullable=False),
    Column("fts_flag", Boolean, nullable=False),
    Column("fts_country_name", Text),
    Column("factoring_mark", Boolean, nullable=False),
    Column("revenue_company", Numeric(20, 2), nullable=False),
    Column("group_id", Text, nullable=False),
    Column("city", Text, nullable=False),
    Column("region", Text, nullable=False),
    UniqueConstraint("source_row", name="uq_company_profiles_source_row"),
)
Index("ix_company_profiles_uid", company_profiles.c.uid)


financial_uid_links = Table(
    "financial_uid_links",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_row", Integer, nullable=False),
    Column("fid", Text, nullable=False),
    Column(
        "uid",
        Text,
        ForeignKey("counterparties.uid", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("city_name", Text),
    Column("rc_name", Text),
    UniqueConstraint("source_row", name="uq_financial_uid_links_source_row"),
)
Index("ix_financial_uid_links_fid", financial_uid_links.c.fid)
Index("ix_financial_uid_links_uid", financial_uid_links.c.uid)


payments = Table(
    "payments",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_row", Integer, nullable=False),
    Column(
        "beneficiary_uid",
        Text,
        ForeignKey("counterparties.uid", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "payer_uid",
        Text,
        ForeignKey("counterparties.uid", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("payment_amount", Numeric(20, 2), nullable=False),
    Column("payment_count", Integer, nullable=False),
    UniqueConstraint("source_row", name="uq_payments_source_row"),
    CheckConstraint("payment_amount >= 0", name="ck_payments_amount_non_negative"),
    CheckConstraint("payment_count >= 0", name="ck_payments_count_non_negative"),
)
Index("ix_payments_beneficiary_uid", payments.c.beneficiary_uid)
Index("ix_payments_payer_uid", payments.c.payer_uid)
Index("ix_payments_beneficiary_payer", payments.c.beneficiary_uid, payments.c.payer_uid)


# These tables are owned by news-feed. The parser owns source configuration and
# only supplies source metadata in Kafka events.
news = Table(
    "news",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_rss_url", Text, nullable=False),
    Column("source_name", Text, nullable=False),
    Column("source_site_url", Text),
    Column("external_id", Text, nullable=False),
    Column("title", Text, nullable=False),
    Column("link", Text, nullable=False),
    Column("content_text", Text),
    Column("authors", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
    Column("published_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("summary", Text),
    Column("section", Text),
    Column("tags", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
    Column("language", Text),
    Column("image_url", Text),
    Column("image_caption", Text),
    Column("images", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
    Column("received_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    UniqueConstraint("source_rss_url", "external_id", name="uq_news_source_external_id"),
)
Index("ix_news_published_at", news.c.published_at)
Index("ix_news_source_rss_url", news.c.source_rss_url)


consumed_news_events = Table(
    "consumed_news_events",
    metadata,
    Column("event_id", UUID(as_uuid=True), primary_key=True),
    Column("received_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
)
