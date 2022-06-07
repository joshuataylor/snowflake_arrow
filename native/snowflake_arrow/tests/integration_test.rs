mod tests {

    use std::fmt::Debug;

    // serialize to rust types
    #[test]
    fn serialize_rust() {
        #[derive(Debug, Clone)]
        enum Foobar<'a, T> {
            Something(Vec<Option<&'a T>>),
        }

        let _bar = Foobar::Something(vec![Some(&1), Some(&2), Some(&3)]);
        let _bar2 = Foobar::Something(vec![Some(&"x"), Some(&"x"), Some(&"x")]);

        // create vec containing bar and bar2
        // let mut vec: Vec<Foobar<'_, Foobar>> = vec![bar, bar2];

        #[derive(Debug)]
        struct GenericVar<T> {
            value: T,
        }

        let dyn_list: Vec<(String, Box<dyn Debug>)> = vec![];

        let _var_u16 = GenericVar::<u16> {
            value: u16::default(),
        };
        let _var_u8 = GenericVar::<u8> {
            value: u8::default(),
        };

        // dyn_list.push(Box::new( var_u8 ));
        // dyn_list.push(Box::new( var_u16 ));

        // dyn_list.push(Box::new( "hello" ));

        println!("{:?}", dyn_list);

        // let vec_of_vecs: Vec<Vec<(String, Foobar)>> = vec![
        //     vec![(String::from("column1"), Foobar::Something(vec![Some(&"a"), Some(&"b"), Some(&"c")]))],
        //     vec![(String::from("column2"), Foobar::Something(vec![Some(&1), Some(&2), Some(&3)]))],
        //     vec![(String::from("column3"), Foobar::Something(vec![Some(&"x"), Some(&"x"), Some(&"x")]))],
        //     vec![(String::from("column1"), Foobar::Something(vec![Some(&"c"), Some(&"d"), Some(&"e")]))],
        // ];

        // let _foo = 1;
        //convert
        // let file = File.read!("/Users/joshtaylor/dev/snowflake_arrow/benchmark/large_arrow")
        //
        // let mut foo = fs::read("/home/josh/dev/snowflake_arrow/benchmark/large_arrow").unwrap();
        // let mut c: &[u8] = &foo; // c: &[u8]
        // // let mut columns: HashMap<String, Vec<Term>> = HashMap::new();
        // //
        // // // We want the metadata later for checking types
        // let metadata = read_stream_metadata(&mut c).unwrap();
        // // for field in &metadata.schema.fields {
        // //     columns.insert(field.name.clone(), vec![]);
        // // }
        // let mut stream = StreamReader::new(&mut c, metadata.clone());
        // let mut chunks: Vec<Chunk<Arc<dyn Array>>> = vec![];
        // //
        // loop {
        //     match stream.next() {
        //         Some(x) => match x {
        //             Ok(StreamState::Some(chunk)) => chunks.push(chunk),
        //             Ok(StreamState::Waiting) => break,
        //             Err(_l) => break,
        //         },
        //         None => break,
        //     }
        // }
        //
        // for chunk in chunks {
        //     let mut field_index = 0;
        //     for array in chunk.iter() {
        //         if array.data_type() == &DataType::Utf8 {
        //             let foo = 1;
        //             let foo = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap().iter().collect::<Vec<Option<&str>>>().iter().for_each(|x| {
        //                 match x {
        //                     Some(x) => println!("{}", x),
        //                     None => (),
        //                 };
        //             });
        //
        //             // let field_metadata = metadata_for_field(&metadata, field_index as u32);
        //
        //             // array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        //         }
        //     }
        //     field_index += 1;
        // }

        //
        // // println!("{:?}", chunks.len());
        // // let b = &a; // b: &Vec<u8>
        // // let c: &[u8] = &foo; // c: &[u8]
        // // let foo = convert(c);
    }

    // #[test]
    // fn vec_borrowed_to_owned() {
    //     let foo: Vec<Option<&i32>> = vec![Some(&3), Some(&4), Some(&5), None];
    //
    //     let converted: Vec<Option<i32>> = foo
    //         .iter()
    //         .map(|x| match x {
    //             Some(y) => Some(*y.to_owned()),
    //             None => None,
    //         })
    //         .collect();
    //
    //     // convert to owned [3, 4, 5]
    //     // let converted = vec![4,5,6];
    //     let foo2: Vec<Option<i32>> = vec![Some(3), Some(4), Some(5), None];
    //
    //     assert_eq!(converted, foo2);
    // }
}
