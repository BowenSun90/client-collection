CREATE TABLE "public"."t_user" (
  "id" int2 NOT NULL,
  "name" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "code" int2 NOT NULL,
  "city" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  CONSTRAINT "t_user_pkey" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."t_user"
OWNER TO "postgres";