CREATE TABLE "article" (
    "article_id" SERIAL PRIMARY KEY,
    "article_title" varchar(50),
    "article_text" text,
    "article_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "article_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "category" (
    "category_id" SERIAL PRIMARY KEY,
    "category_title" varchar(50),
    "category_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "category_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "tag" (
    "tag_id" SERIAL PRIMARY KEY,
    "tag_value" varchar(50),
    "tag_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "tag_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "article__tag" (
    "article_id" INTEGER NOT NULL,
    "tag_id" INTEGER NOT NULL,
    PRIMARY KEY ("article_id", "tag_id")
);
 
ALTER TABLE "article" ADD "category_id" INTEGER NOT NULL, 
    ADD CONSTRAINT "fk_article_category_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
 
ALTER TABLE "article__tag"
    ADD CONSTRAINT "fk_article__tag_article_id" FOREIGN KEY ("article_id") REFERENCES "article" ("article_id");
 
ALTER TABLE "tag__article"
    ADD CONSTRAINT "fk_tag__article_tag_id" FOREIGN KEY ("tag_id") REFERENCES "tag" ("tag_id");
 
CREATE OR REPLACE FUNCTION update_article_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.article_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_article_updated" BEFORE UPDATE ON "article" FOR EACH ROW EXECUTE PROCEDURE update_article_timestamp();
 
CREATE OR REPLACE FUNCTION update_category_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.category_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_category_updated" BEFORE UPDATE ON "category" FOR EACH ROW EXECUTE PROCEDURE update_category_timestamp();
 
CREATE OR REPLACE FUNCTION update_tag_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tag_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_tag_updated" BEFORE UPDATE ON "tag" FOR EACH ROW EXECUTE PROCEDURE update_tag_timestamp();
 
CREATE TABLE "article" (
    "article_id" SERIAL PRIMARY KEY,
    "article_title" varchar(50),
    "article_text" text,
    "article_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "article_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "category" (
    "category_id" SERIAL PRIMARY KEY,
    "category_title" varchar(50),
    "category_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "category_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "tag" (
    "tag_id" SERIAL PRIMARY KEY,
    "tag_value" varchar(50),
    "tag_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "tag_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "article__tag" (
    "article_id" INTEGER NOT NULL,
    "tag_id" INTEGER NOT NULL,
    PRIMARY KEY ("article_id", "tag_id")
);
 
ALTER TABLE "article" ADD "category_id" INTEGER NOT NULL, 
    ADD CONSTRAINT "fk_article_category_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
 
ALTER TABLE "article__tag"
    ADD CONSTRAINT "fk_article__tag_article_id" FOREIGN KEY ("article_id") REFERENCES "article" ("article_id");
 
ALTER TABLE "tag__article"
    ADD CONSTRAINT "fk_tag__article_tag_id" FOREIGN KEY ("tag_id") REFERENCES "tag" ("tag_id");
 
CREATE OR REPLACE FUNCTION update_article_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.article_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_article_updated" BEFORE UPDATE ON "article" FOR EACH ROW EXECUTE PROCEDURE update_article_timestamp();
 
CREATE OR REPLACE FUNCTION update_category_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.category_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_category_updated" BEFORE UPDATE ON "category" FOR EACH ROW EXECUTE PROCEDURE update_category_timestamp();
 
CREATE OR REPLACE FUNCTION update_tag_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tag_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_tag_updated" BEFORE UPDATE ON "tag" FOR EACH ROW EXECUTE PROCEDURE update_tag_timestamp();
 
CREATE TABLE "article" (
    "article_id" SERIAL PRIMARY KEY,
    "article_title" varchar(50),
    "article_text" text,
    "article_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "article_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "category" (
    "category_id" SERIAL PRIMARY KEY,
    "category_title" varchar(50),
    "category_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "category_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "tag" (
    "tag_id" SERIAL PRIMARY KEY,
    "tag_value" varchar(50),
    "tag_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "tag_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "article__tag" (
    "article_id" INTEGER NOT NULL,
    "tag_id" INTEGER NOT NULL,
    PRIMARY KEY ("article_id", "tag_id")
);
 
ALTER TABLE "article" ADD "category_id" INTEGER NOT NULL, 
    ADD CONSTRAINT "fk_article_category_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
 
ALTER TABLE "article__tag"
    ADD CONSTRAINT "fk_article__tag_article_id" FOREIGN KEY ("article_id") REFERENCES "article" ("article_id");
 
ALTER TABLE "tag__article"
    ADD CONSTRAINT "fk_tag__article_tag_id" FOREIGN KEY ("tag_id") REFERENCES "tag" ("tag_id");
 
CREATE OR REPLACE FUNCTION update_article_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.article_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_article_updated" BEFORE UPDATE ON "article" FOR EACH ROW EXECUTE PROCEDURE update_article_timestamp();
 
CREATE OR REPLACE FUNCTION update_category_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.category_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_category_updated" BEFORE UPDATE ON "category" FOR EACH ROW EXECUTE PROCEDURE update_category_timestamp();
 
CREATE OR REPLACE FUNCTION update_tag_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tag_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_tag_updated" BEFORE UPDATE ON "tag" FOR EACH ROW EXECUTE PROCEDURE update_tag_timestamp();
 
CREATE TABLE "article" (
    "article_id" SERIAL PRIMARY KEY,
    "article_title" varchar(50),
    "article_text" text,
    "article_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "article_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "category" (
    "category_id" SERIAL PRIMARY KEY,
    "category_title" varchar(50),
    "category_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "category_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "tag" (
    "tag_id" SERIAL PRIMARY KEY,
    "tag_value" varchar(50),
    "tag_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "tag_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "article__tag" (
    "article_id" INTEGER NOT NULL,
    "tag_id" INTEGER NOT NULL,
    PRIMARY KEY ("article_id", "tag_id")
);
 
ALTER TABLE "article" ADD "category_id" INTEGER NOT NULL, 
    ADD CONSTRAINT "fk_article_category_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
 
ALTER TABLE "article__tag"
    ADD CONSTRAINT "fk_article__tag_article_id" FOREIGN KEY ("article_id") REFERENCES "article" ("article_id");
 
ALTER TABLE "tag__article"
    ADD CONSTRAINT "fk_tag__article_tag_id" FOREIGN KEY ("tag_id") REFERENCES "tag" ("tag_id");
 
CREATE OR REPLACE FUNCTION update_article_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.article_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_article_updated" BEFORE UPDATE ON "article" FOR EACH ROW EXECUTE PROCEDURE update_article_timestamp();
 
CREATE OR REPLACE FUNCTION update_category_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.category_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_category_updated" BEFORE UPDATE ON "category" FOR EACH ROW EXECUTE PROCEDURE update_category_timestamp();
 
CREATE OR REPLACE FUNCTION update_tag_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tag_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_tag_updated" BEFORE UPDATE ON "tag" FOR EACH ROW EXECUTE PROCEDURE update_tag_timestamp();
 
CREATE TABLE "article" (
    "article_id" SERIAL PRIMARY KEY,
    "article_title" varchar(50),
    "article_text" text,
    "article_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "article_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "category" (
    "category_id" SERIAL PRIMARY KEY,
    "category_title" varchar(50),
    "category_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "category_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "tag" (
    "tag_id" SERIAL PRIMARY KEY,
    "tag_value" varchar(50),
    "tag_created" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER),
    "tag_updated" INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)
);
 
CREATE TABLE "article__tag" (
    "article_id" INTEGER NOT NULL,
    "tag_id" INTEGER NOT NULL,
    PRIMARY KEY ("article_id", "tag_id")
);
 
ALTER TABLE "article" ADD "category_id" INTEGER NOT NULL, 
    ADD CONSTRAINT "fk_article_category_id" FOREIGN KEY ("category_id") REFERENCES "category" ("category_id");
 
ALTER TABLE "article__tag"
    ADD CONSTRAINT "fk_article__tag_article_id" FOREIGN KEY ("article_id") REFERENCES "article" ("article_id");
 
ALTER TABLE "tag__article"
    ADD CONSTRAINT "fk_tag__article_tag_id" FOREIGN KEY ("tag_id") REFERENCES "tag" ("tag_id");
 
CREATE OR REPLACE FUNCTION update_article_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.article_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_article_updated" BEFORE UPDATE ON "article" FOR EACH ROW EXECUTE PROCEDURE update_article_timestamp();
 
CREATE OR REPLACE FUNCTION update_category_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.category_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_category_updated" BEFORE UPDATE ON "category" FOR EACH ROW EXECUTE PROCEDURE update_category_timestamp();
 
CREATE OR REPLACE FUNCTION update_tag_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tag_updated = cast(extract(epoch from now()) as integer);
    RETURN NEW;
END;
$$ language "plpgsql";
CREATE TRIGGER "tr_tag_updated" BEFORE UPDATE ON "tag" FOR EACH ROW EXECUTE PROCEDURE update_tag_timestamp();
 
