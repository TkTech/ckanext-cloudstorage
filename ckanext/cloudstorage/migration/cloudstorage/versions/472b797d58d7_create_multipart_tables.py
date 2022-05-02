"""Create multipart tables

Revision ID: 472b797d58d7
Revises:
Create Date: 2021-01-12 14:24:02.227319

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = "472b797d58d7"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if "cloudstorage_multipart_upload" not in tables:
        op.create_table(
            "cloudstorage_multipart_upload",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column("resource_id", sa.UnicodeText),
            sa.Column("name", sa.UnicodeText),
            sa.Column("initiated", sa.DateTime),
            sa.Column("size", sa.Numeric),
            sa.Column("original_name", sa.UnicodeText),
            sa.Column("user_id", sa.UnicodeText),
        )

    if "cloudstorage_multipart_part" not in tables:
        op.create_table(
            "cloudstorage_multipart_part",
            sa.Column("n", sa.Integer, primary_key=True),
            sa.Column("etag", sa.UnicodeText, primary_key=True),
            sa.Column(
                "upload_id",
                sa.UnicodeText,
                sa.ForeignKey("cloudstorage_multipart_upload.id"),
                primary_key=True,
            ),
        )


def downgrade():
    op.drop_table("cloudstorage_multipart_part")
    op.drop_table("cloudstorage_multipart_upload")
