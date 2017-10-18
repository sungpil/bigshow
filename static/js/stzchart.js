var sundaytoz = {chart:{}}

sundaytoz.chart.RetentionTable = class RetentionTable {
	constructor(container) {
		this._parent = container;
	}
	draw(data, option) {
		console.log(this._parent);
		console.log(data);
		var table_tpl = '<div class="stz-graph-retention" style="width:100%; height:{height}; overflow:auto"><table class="stz-graph-retention-table">{header}{header-avg}{data-rows}</table></div>';
		var height = (option.height.toString().includes('%')) ? option.height : option.height + 'px';
		var header = this.makeHeader(data.shift());
		var rttAndAvg = this.makeRttAndAvg(data);
		var table = table_tpl.replace('{header}',header)
				.replace('{header-avg}',this.makeHeader(rttAndAvg.avg))
				.replace('{data-rows}',this.makeDataRow(rttAndAvg.rtt)).replace('{height}',height);
		$(this._parent).append(table);
	}
	makeHeader(data) {
		var tr = '<tr>{th-list}</tr>';
		var th_list = [];
		data.forEach(function(datum) {
			th_list.push('<th>'+datum+'</th>');
		});
		return tr.replace('{th-list}',th_list.join(''));
	}
	makeDataRow(data) {
		var tr_list = [];
		var tr = '<tr>{td-list}</tr>';
		data.forEach(function(row) {
			var td_list = [];
			var date = row[0];
			td_list.push('<td class="col-date">'+date+'<br/>'+row[1].value+'</td>');
			for(var i=1,len=row.length;i<len;i++) {
				var rtt = row[i].rtt;
				var data_str = (rtt > 0) ? rtt+'%' : '&nbsp;';
				var data_class = '';
				if( rtt >= 100 ) {
					data_class = 'blue-5';
				} else if( rtt > 45 ) {
					data_class = 'blue-4';
				} else if( rtt > 40 ) {
					data_class = 'blue-3';
				} else if(rtt > 25) {
					data_class = 'blue-2';
				} else if(rtt > 12) {
					data_class = 'blue-1';
				} else if(rtt > 0) {
					data_class = 'blue-0';
				}
				td_list.push('<td class="col-data '+data_class+'" tootip="'+row[i].value+'">'+ data_str +'</td>');
			}
			tr_list.push(tr.replace('{td-list}', td_list.join('')));
		});
		return tr_list.join('');
	}
	makeRttAndAvg(data) {
		var rtt_list = [];
		var rtt_sum_list= [];
		var avg = ['','100%'];
		var len = data[0].length-2;
		while(len-- > 0) {
			rtt_sum_list.push([]);
		}
		data.forEach(function(row){
			var result_row = [];
			var nru = row[1];
			result_row.push(row[0]);
			result_row.push({'value':nru,'rtt':'100'});
			for(var i=2; i<row.length; i++) {
				var dru = row[i];
				if(dru > 0) {
					var rtt = dru/nru*100
					rtt_sum_list[i-2].push(rtt);
					result_row.push({'value':row[i],'rtt':rtt.toFixed(1)});
				} else {
					result_row.push({'value':'','rtt':''});
				}
			}
			rtt_list.push(result_row);
		});
		rtt_sum_list.forEach(function(rtt_sum){
			if(rtt_sum && rtt_sum.length > 0) {
				var sum = 0;
				rtt_sum.forEach(function(rtt) {
					sum+=rtt;
				});
				avg.push((sum/rtt_sum.length).toFixed(1)+'%');
			} else {
				avg.push('');
			}
		});
		return {'rtt':rtt_list, 'avg':avg};
	}
}
