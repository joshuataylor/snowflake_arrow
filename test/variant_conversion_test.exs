defmodule SnowflakeArrow.VariantConversionTest do
  use ExUnit.Case, async: true

  test "Can convert variant to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_VARIANT_JSON.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data)

    assert values == [
             [nil],
             [nil],
             [nil],
             [nil],
             ["{\n  \"key_l8JNh6cOrmsCxEl94Cf5\": true\n}"],
             ["{\n  \"key_jz85OWJBXLoduI8IepHj\": true\n}"],
             ["{\n  \"key_rG26wp4t2LaxsilFarH8\": true\n}"],
             ["{\n  \"key_yl08GudsydgGeS1nxvze\": true\n}"],
             ["{\n  \"key_GEHHwmWyxK3dfqFUWeCS\": true\n}"],
             [nil],
             [nil]
           ]
  end

  test "Can convert array to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_ARRAY.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data)

    assert values == [
             [nil],
             [nil],
             ["[\n  12,\n  \"twelve\",\n  undefined\n]"],
             [nil],
             [nil],
             [nil],
             ["[\n  12,\n  \"twelve\",\n  undefined\n]"],
             ["[\n  12,\n  \"twelve\",\n  undefined\n]"],
             [nil],
             ["[\n  12,\n  \"twelve\",\n  undefined\n]"],
             [nil]
           ]
  end

  test "Can convert object to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_OBJECT.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data)

    assert values == [
             [nil],
             ["{\n  \"arr1_IQqqQ\": 13,\n  \"zero\": 0\n}"],
             ["{\n  \"arr1_2xgjR\": 13,\n  \"zero\": 0\n}"],
             [nil],
             [nil],
             [nil],
             ["{\n  \"arr1_BRCwF\": 13,\n  \"zero\": 0\n}"],
             ["{\n  \"arr1_v3yFa\": 13,\n  \"zero\": 0\n}"],
             ["{\n  \"arr1_zNuUH\": 13,\n  \"zero\": 0\n}"],
             ["{\n  \"arr1_p1Ja8\": 13,\n  \"zero\": 0\n}"],
             ["{\n  \"arr1_FmKXQ\": 13,\n  \"zero\": 0\n}"]
           ]
  end
end
