
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeCodec;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListSet;


public class ScanFile {
    private final byte[] qualifierBytes = "visitTimeCost".getBytes();
    private Configuration conf;

    public  ScanFile(){
        conf = HBaseConfiguration.create();
    }

    private Configuration getConf() {
        return conf;
    }

    private void processFile(Path file) throws IOException {

        FileSystem fs = file.getFileSystem(getConf());
        if (!fs.exists(file)) {
            System.err.println("ERROR, file doesnt exist: " + file);
            System.exit(-2);
        }

        Cell seekCell = createSeekCell();
        HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(getConf()), getConf());
        HFileScanner scanner = reader.getScanner(false, false, false);

        boolean shouldScanKeysValues = (scanner.seekTo(seekCell) != -1);
        if(!shouldScanKeysValues)
            return;

        //final byte[] qualifierBytes = "serverIP".getBytes();
        do{
            Cell cell = scanner.getKeyValue();
            int cmp = CellComparator.compareRows(cell,seekCell);
            if (cmp < 0){
                continue;
            }
            System.out.println(cell);
            if( Bytes.compareTo(cell.getQualifierArray(),qualifierBytes) == 0){
                break;
            }
        }while (scanner.next());

        Cell x = getKeyForNextColumn(scanner.getKeyValue());
        scanner.reseekTo(x);
        System.out.println(scanner.getKeyValue());

        if(CellComparator.compareRows(scanner.getKeyValue(),seekCell) < 0)
            System.out.println("ERROR !!!");

    }
    public Cell getKeyForNextColumn(Cell kv) {
        return KeyValueUtil.createLastOnRow(
            kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
            kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
            kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
      }

    private Cell createSeekCell() {
        final  String rowStr = "43015_1448270710720_141307_yhd.orders.get_2_606101";
        //final  String rowStr = "43015_1448270710720_141307_yhd.orders.get_2_610205";
        byte[] row = rowStr.getBytes();
        row[0] = 4;
        return KeyValueUtil.createFirstOnRow(row);
    }

    private Cell createAnotherSeekCell() {
        final  String rowStr = "43015_1448270710720_141307_yhd.orders.get_2_606101";
        //final  String rowStr = "43015_1448270710720_141307_yhd.orders.get_2_610205";
        byte[] row = rowStr.getBytes();
        return KeyValueUtil.createFirstOnRow(row);
    }

    private static ConcurrentSkipListSet<Cell>  generateRandomTestData(){
        ConcurrentSkipListSet<Cell> kvset = new ConcurrentSkipListSet<Cell>(
            KeyValue.COMPARATOR);

        //row
        String rows[] = {
//            "43015_1448270710720_141307_yhd.orders.get_2_551535",
//            "43015_1448270710720_141307_yhd.orders.get_2_556602",
//            "43015_1448270710720_141307_yhd.orders.get_2_580344",
//            "43015_1448270710720_141307_yhd.orders.get_2_591946",
//            "43015_1448270710720_141307_yhd.orders.get_2_597265",
            "43015_1448270710720_141307_yhd.orders.get_2_6",
            "43015_1448270710720_141307_yhd.orders.get_2_606101",
            "43015_1448270710720_141307_yhd.orders.get_2_610205",
            "43015_1448270710720_141307_yhd.orders.get_2_614364"};
//            "43015_1448270710720_141307_yhd.orders.get_2_626901",
//            "43015_1448270710720_141307_yhd.orders.get_2_663775",
//            "43015_1448270710720_141307_yhd.orders.get_2_665115",
//            "43015_1448270710720_141307_yhd.orders.get_2_679778",
//            "43015_1448270710720_141307_yhd.orders.get_2_682224",
//            "43015_1448270710720_141307_yhd.orders.get_2_685492"};

        byte[][] rowBytes = new byte[rows.length][];
        for( int i = 0; i < rows.length; i++){
            rowBytes[i] = rows[i].getBytes();
        }

        //column family
        byte[] CF_BYTES = Bytes.toBytes("openapi");

        //qualifier
        String qualifiers[] = {
            "amount","apiType","categoryId","createTime","invokeMethod","invokeStatus","ip","isFree","isvId","merchantId","methodVer","requestValue",
            "responseValue","resultCode","resultType","ruleVer","serverIP","visitTimeCost"};
        byte[][] qualifierBytes = new byte[qualifiers.length][];

        for( int i = 0; i < qualifiers.length; i++){
            qualifierBytes[i] = qualifiers[i].getBytes();
        }

        // value
        byte[] value = new byte[]{1};
        // create kv
        for (int i = 0; i < rowBytes.length;i++){
            for (int j = 0; j < qualifierBytes.length;j++){
                KeyValue kv = new KeyValue(rowBytes[i],CF_BYTES,qualifierBytes[j],System.currentTimeMillis(),value);
                kvset.add(kv);
            }
        }
        return kvset;
    }
    private static
    void encodeData(ConcurrentSkipListSet<Cell> kvset, PrefixTreeCodec encoder,HFileBlockEncodingContext blkEncodingCtx, DataOutputStream userDataStream) throws Exception {
        encoder.startBlockEncoding(blkEncodingCtx, userDataStream);
        for (Cell kv : kvset) {
            encoder.encode(kv, blkEncodingCtx, userDataStream);
        }
        encoder.endBlockEncoding(blkEncodingCtx, userDataStream, null);
    }
    private  void test() throws Exception {
        PrefixTreeCodec encoder = new PrefixTreeCodec();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);

        HFileContext meta = new HFileContextBuilder()
            .withHBaseCheckSum(false)
            .withIncludesMvcc(false)
            .withIncludesTags(false)
            .withCompression(Compression.Algorithm.NONE)
            .build();
        HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
            DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
        ConcurrentSkipListSet<Cell> kvset = generateRandomTestData();
        encodeData(kvset, encoder, blkEncodingCtx, userDataStream);

        DataBlockEncoder.EncodedSeeker encodeSeeker = encoder.createSeeker(KeyValue.COMPARATOR,
            encoder.newDataBlockDecodingContext(meta));
        byte[] onDiskBytes = baosInMemory.toByteArray();
        ByteBuffer encodedData = ByteBuffer.wrap(onDiskBytes, DataBlockEncoding.ID_SIZE,
            onDiskBytes.length - DataBlockEncoding.ID_SIZE);
        encodeSeeker.setCurrentBuffer(encodedData);

        Cell seekCell = createAnotherSeekCell();
        encodeSeeker.seekToKeyInBlock(seekCell,false);
        do {
            Cell cell = encodeSeeker.getKeyValue();
            int cmp = CellComparator.compareRows(cell,seekCell);
            if (cmp < 0){
                continue;
            }
            System.out.println(encodeSeeker.getKeyValue());
            if( Bytes.compareTo(cell.getQualifierArray(),qualifierBytes) == 0){
                break;
            }
        }while (encodeSeeker.next());

        Cell x = getKeyForNextColumn(encodeSeeker.getKeyValue());
        encodeSeeker.seekToKeyInBlock(x,false);
        System.out.println(encodeSeeker.getKeyValue());

        if(CellComparator.compareRows(encodeSeeker.getKeyValue(),x) < 0)
            System.out.println("ERROR !!!");

    }
    public static void main(String[] args) throws Exception {

        new ScanFile().test();
        /*
        if (args.length < 1){
            System.err.println("ERROR, please specify: file");
            System.exit(-2);
        }

        new ScanFile().processFile(new Path(args[0]));
        */
    }
}
