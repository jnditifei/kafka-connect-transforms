# Kafka Connect Single Message Transforms (SMTs)

This repository provides two custom Single Message Transforms (SMTs) for Kafka Connect:

SetMetadataNested: Updates the schema metadata of a nested struct dynamically.

NestFields: Moves specified fields into a new nested struct.

## 📌 SetMetadataNested SMT
Description
The SetMetadataNested SMT allows dynamically updating the schema metadata (name, version, documentation) of a specified nested struct field. This is useful when renaming or restructuring schemas without altering data.

| Config Key         |   Type   | Required |                                                                                                           Description |
|:-------------------|:--------:|---------:|----------------------------------------------------------------------------------------------------------------------:|
| nested.schema.name |  String  |    ✅ Yes |                                                                   The new name to assign to the nested struct schema. |
| field.name         |  String  |    ✅ Yes |                                                   The name of the nested struct field whose schema should be updated. |

```javascript
transforms=SetMetadataNested
transforms.SetMetadataNested.type=com.jnditifei.kafka.connect.transforms.SetMetadataNested
transforms.SetMetadataNested.field.name=before
transforms.SetMetadataNested.nested.schema.name=com.mycompagny.class
```


## 📌 NestFields SMT
Description
The NestFields SMT takes one or more top-level fields and moves them into a new nested struct. This is useful when restructuring records before processing.

| Config Key        |  Type  | Required |                                                                  Description |
|:------------------|:--------:|---------:|-----------------------------------------------------------------------------:|
| NestFields.fields | String |    ✅ Yes |           Comma-separated list of field names to move into the nested field. |


```javascript
transforms=NestFields
transforms.NestFields.type=com.jnditifei.kafka.connect.transforms.NestFields
transforms.NestFields.fields.to.nest=customer_id:customer_info,email:contact_info
```

### Example Input → Output
Input Record (before transformation)

```javascript
{
  "customer_id": 12345,
  "name": "Alice",
  "email": "alice@example.com",
  "address": "123 Main St"
}
```

Output Record (after transformation)

```javascript
{
  "customer_info": {
    "customer_id": 12345
  },
  "name": "Alice",
  "contact_info": {
    "email": "alice@example.com"
  },
  "address": "123 Main St"
}
```

## 🛠 Installation & Deployment

### Build the JAR
Clone the repository:
git clone https://github.com/jnditifei/kafka-connect-transforms.git
cd kafka-connect-transforms
Build the JAR using Maven:

mvn clean package
Copy the JAR to your Kafka Connect plugins directory:

cp target/kafka-connect-transforms.jar /path/to/kafka-connect/plugins/
Restart Kafka Connect.

🎯 Usage in Kafka Connect
Add the SMT to your Kafka Connect connector configuration

### 👨‍💻 Contributing
Feel free to submit issues, feature requests, or pull requests to improve these SMTs.

### 📜 License
This project is licensed under the MIT License.